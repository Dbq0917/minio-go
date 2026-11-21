/*
 * MinIO Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2026 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package minio

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"
)

// zeroSource is an io.Reader and io.ReaderAt of size zero bytes; it keeps a
// multi-GiB "file" from costing any memory.
type zeroSource struct {
	size int64
	off  int64
}

func (r *zeroSource) Read(p []byte) (int, error) {
	if r.off >= r.size {
		return 0, io.EOF
	}
	n := min(int64(len(p)), r.size-r.off)
	clear(p[:n])
	r.off += n
	return int(n), nil
}

func (r *zeroSource) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.size {
		return 0, io.EOF
	}
	n := min(int64(len(p)), r.size-off)
	clear(p[:n])
	return int(n), nil
}

// partConcurrencyTransport counts the part PUTs the client has in flight.
type partConcurrencyTransport struct {
	base http.RoundTripper
	cur  atomic.Int32
	max  atomic.Int32
}

func (t *partConcurrencyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	tracked := r.Method == http.MethodPut && r.URL.Query().Has("partNumber")
	if tracked {
		cur := t.cur.Add(1)
		for {
			old := t.max.Load()
			if cur <= old || t.max.CompareAndSwap(old, cur) {
				break
			}
		}
		defer t.cur.Add(-1)
	}
	return t.base.RoundTrip(r)
}

// newConstrainedUploadServer emulates a server that admits at most
// serverSlots concurrent part bodies and queues at most maxWaiting more.
// A part arriving when the queue is full is rejected with 503, the way an
// exhausted connection pool surfaces as request failures. The limit is on
// concurrency, not wall-clock time, so the behavior is deterministic.
func newConstrainedUploadServer(t *testing.T, serverSlots, maxWaiting int, partService time.Duration) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	slots := make(chan struct{}, serverSlots)
	var waiting atomic.Int32
	rejected := &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Query().Has("uploads"):
			w.Write([]byte(`<InitiateMultipartUploadResult><UploadId>id</UploadId></InitiateMultipartUploadResult>`))
		case r.Method == http.MethodPut:
			if waiting.Add(1) > int32(maxWaiting) {
				waiting.Add(-1)
				rejected.Add(1)
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			select {
			case slots <- struct{}{}:
				waiting.Add(-1)
				time.Sleep(partService)
				io.Copy(io.Discard, r.Body)
				<-slots
				w.Header().Set("ETag", `"etag"`)
			case <-r.Context().Done():
				waiting.Add(-1)
				rejected.Add(1)
			}
		case r.Method == http.MethodPost:
			w.Write([]byte(`<CompleteMultipartUploadResult><Bucket>bucket</Bucket></CompleteMultipartUploadResult>`))
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, rejected
}

// Three 2 GiB objects uploaded at once with NumThreads=32 each must share
// the client's part-upload budget: without the scheduler they drive 96
// concurrent part PUTs at a server that admits 16 and queues 24, and the
// overload rejects them. With the budget shared fairly all three succeed,
// no upload is starved, and the client never has more than NumThreads parts
// in flight, so at most 16 of them ever wait at the server.
func TestPutObjectConcurrentUploadsShareBudget(t *testing.T) {
	const (
		fileSize    = 2 << 30 // 2 GiB
		numThreads  = 32
		serverSlots = 16
		maxWaiting  = 24
		partService = 50 * time.Millisecond
		uploads     = 3
	)

	srv, rejected := newConstrainedUploadServer(t, serverSlots, maxWaiting, partService)
	transport := &partConcurrencyTransport{base: http.DefaultTransport}
	clnt, err := New(srv.Listener.Addr().String(), &Options{
		Creds:     credentials.NewStaticV4("foo", "foo12345", ""),
		Region:    "us-east-1",
		Transport: transport,
	})
	if err != nil {
		t.Fatal(err)
	}
	clnt.maxRetries = 1

	var wg sync.WaitGroup
	errs := make([]error, uploads)
	elapsed := make([]time.Duration, uploads)
	start := time.Now()
	for i := 0; i < uploads; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			t0 := time.Now()
			_, errs[i] = clnt.PutObject(t.Context(), "bucket", fmt.Sprintf("object-%d", i),
				&zeroSource{size: fileSize}, fileSize, PutObjectOptions{NumThreads: numThreads})
			elapsed[i] = time.Since(t0)
		}(i)
	}
	wg.Wait()
	total := time.Since(start)

	failures := 0
	for i, err := range errs {
		if err != nil {
			t.Errorf("upload %d failed: %v", i, err)
			failures++
		}
	}
	t.Logf("%d x %d GiB uploads: %d failures, %d rejected parts, total %v, per-file %v",
		uploads, fileSize>>30, failures, rejected.Load(), total.Round(time.Millisecond), elapsed)

	if n := rejected.Load(); n != 0 {
		t.Errorf("server rejected %d parts, want 0", n)
	}
	if got := transport.max.Load(); got > numThreads {
		t.Errorf("max concurrent part PUTs = %d, want at most %d (the shared budget)", got, numThreads)
	}
	// No upload is starved: the slowest file finishes well within twice the
	// fastest, and the aggregate time stays close to what the server's
	// admission limit allows.
	slowest, fastest := elapsed[0], elapsed[0]
	for _, d := range elapsed[1:] {
		slowest = max(slowest, d)
		fastest = min(fastest, d)
	}
	if slowest > 2*fastest {
		t.Errorf("slowest upload %v is more than twice the fastest %v", slowest, fastest)
	}
	serverBound := time.Duration(uploads*(fileSize/(5<<20))/serverSlots) * partService
	if total > 2*serverBound {
		t.Errorf("total elapsed %v is more than twice the server's throughput bound %v", total, serverBound)
	}
}

// A single upload on an otherwise idle client still gets its full NumThreads
// worth of parallelism, so its throughput is unchanged by the sharing.
func TestPutObjectSingleUploadKeepsFullBudget(t *testing.T) {
	const (
		fileSize    = 2 << 30 // 2 GiB
		numThreads  = 32
		serverSlots = 16
		maxWaiting  = 24
		partService = 50 * time.Millisecond
	)

	srv, rejected := newConstrainedUploadServer(t, serverSlots, maxWaiting, partService)
	transport := &partConcurrencyTransport{base: http.DefaultTransport}
	clnt, err := New(srv.Listener.Addr().String(), &Options{
		Creds:     credentials.NewStaticV4("foo", "foo12345", ""),
		Region:    "us-east-1",
		Transport: transport,
	})
	if err != nil {
		t.Fatal(err)
	}
	clnt.maxRetries = 1

	start := time.Now()
	if _, err = clnt.PutObject(t.Context(), "bucket", "object",
		&zeroSource{size: fileSize}, fileSize, PutObjectOptions{NumThreads: numThreads}); err != nil {
		t.Fatal(err)
	}
	total := time.Since(start)
	t.Logf("single %d GiB upload: %v, %d rejected parts", fileSize>>30, total.Round(time.Millisecond), rejected.Load())

	if n := rejected.Load(); n != 0 {
		t.Errorf("server rejected %d parts, want 0", n)
	}
	// The lone upload may use its whole budget, not a fair-share fraction.
	if got := transport.max.Load(); got > numThreads {
		t.Errorf("max concurrent part PUTs = %d, want at most %d", got, numThreads)
	} else if got < serverSlots {
		t.Errorf("max concurrent part PUTs = %d, want at least the server's %d slots used", got, serverSlots)
	}
}
