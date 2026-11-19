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
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/minio/minio-go/v7/pkg/credentials"
)

// flakyUploadHandler fails the first failPerPart PUT attempts of every part
// with a retryable 503, then accepts the part. It records every part number
// the client sent so tests can prove no part number is duplicated on
// completion and that transient failures are retried at the part level.
func flakyUploadHandler(t *testing.T, failPerPart int, failCode string) (http.HandlerFunc, *atomic.Int32, map[int]int) {
	t.Helper()
	var mu sync.Mutex
	attemptsByPart := make(map[int]int)
	var completed atomic.Int32

	handler := func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Query().Has("uploads"):
			w.Write([]byte(`<InitiateMultipartUploadResult><UploadId>id</UploadId></InitiateMultipartUploadResult>`))
		case r.Method == http.MethodPut:
			n, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))
			io.Copy(io.Discard, r.Body)
			mu.Lock()
			attemptsByPart[n]++
			attempt := attemptsByPart[n]
			mu.Unlock()
			if attempt <= failPerPart {
				w.WriteHeader(http.StatusServiceUnavailable)
				if failCode != "" {
					io.WriteString(w, `<Error><Code>`+failCode+`</Code><Message>slow down</Message></Error>`)
				}
				return
			}
			w.Header().Set("ETag", `"etag`+strconv.Itoa(n)+`"`)
		case r.Method == http.MethodPost:
			body, _ := io.ReadAll(r.Body)
			completed.Store(int32(strings.Count(string(body), "<Part>")))
			w.Write([]byte(`<CompleteMultipartUploadResult><Bucket>bucket</Bucket></CompleteMultipartUploadResult>`))
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}
	return handler, &completed, attemptsByPart
}

func newRetryTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	clnt, err := New(srv.Listener.Addr().String(), &Options{
		Creds:      credentials.NewStaticV4("foo", "foo12345", ""),
		Region:     "us-east-1",
		MaxRetries: 1, // disable transport-level retries to isolate part-level retries
	})
	if err != nil {
		t.Fatal(err)
	}
	return clnt
}

// TestParallelStreamPartUploadRetriesIdempotent drives the
// ConcurrentStreamParts uploader with 16 workers against a server that fails
// every part's first attempt. The upload must succeed, every part must be
// retried under the same part number, and CompleteMultipartUpload must carry
// exactly one entry per part number.
func TestParallelStreamPartUploadRetriesIdempotent(t *testing.T) {
	const (
		partSize = 5 << 20
		workers  = 16
		parts    = 24
	)
	handler, completed, attemptsByPart := flakyUploadHandler(t, 1, "SlowDown")
	clnt := newRetryTestClient(t, handler)

	r := bytes.NewReader(make([]byte, parts*partSize))
	info, err := clnt.PutObject(t.Context(), "bucket", "object", r, int64(parts*partSize), PutObjectOptions{
		PartSize:              partSize,
		ConcurrentStreamParts: true,
		NumThreads:            workers,
	})
	if err != nil {
		t.Fatalf("PutObject failed despite part-level retries: %v", err)
	}
	if info.Size != int64(parts*partSize) {
		t.Errorf("Size = %d, want %d", info.Size, parts*partSize)
	}
	if got := completed.Load(); got != parts {
		t.Errorf("completed parts = %d, want %d (duplicate or missing part numbers)", got, parts)
	}
	for n := 1; n <= parts; n++ {
		got, ok := attemptsByPart[n]
		if !ok {
			t.Errorf("part %d was never uploaded", n)
			continue
		}
		if got != 2 {
			t.Errorf("part %d upload attempts = %d, want 2 (one failure + one retry)", n, got)
		}
	}
}

// TestReadAtPartUploadRetriesIdempotent covers the ReadAt-based parallel
// uploader with the same flaky server, including the non-uniform last part.
func TestReadAtPartUploadRetriesIdempotent(t *testing.T) {
	const (
		partSize = 5 << 20
		workers  = 16
		parts    = 20
	)
	// One extra byte makes the final part a different size, exercising the
	// last-part geometry that used to race on the shared partSize variable.
	totalSize := int64((parts-1)*partSize + 1)
	handler, completed, attemptsByPart := flakyUploadHandler(t, 1, "SlowDown")
	clnt := newRetryTestClient(t, handler)

	r := bytes.NewReader(make([]byte, totalSize))
	info, err := clnt.PutObject(t.Context(), "bucket", "object", r, totalSize, PutObjectOptions{
		PartSize:   partSize,
		NumThreads: workers,
	})
	if err != nil {
		t.Fatalf("PutObject failed despite part-level retries: %v", err)
	}
	if info.Size != totalSize {
		t.Errorf("Size = %d, want %d", info.Size, totalSize)
	}
	if got := completed.Load(); got != parts {
		t.Errorf("completed parts = %d, want %d (duplicate or missing part numbers)", got, parts)
	}
	for n := 1; n <= parts; n++ {
		if got := attemptsByPart[n]; got != 2 {
			t.Errorf("part %d upload attempts = %d, want 2", n, got)
		}
	}
}

// TestPartUploadRetryExhausted surfaces a persistent failure instead of
// retrying forever or completing with a missing part.
func TestPartUploadRetryExhausted(t *testing.T) {
	const (
		partSize = 5 << 20
		workers  = 4
		parts    = 6
	)
	// Every attempt fails with a retryable 503; partUploadMaxAttempts must give
	// up and the whole multipart upload must be aborted.
	handler, _, attemptsByPart := flakyUploadHandler(t, partUploadMaxAttempts, "SlowDown")
	var aborts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			aborts.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	clnt, err := New(srv.Listener.Addr().String(), &Options{
		Creds:      credentials.NewStaticV4("foo", "foo12345", ""),
		Region:     "us-east-1",
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	r := bytes.NewReader(make([]byte, parts*partSize))
	_, err = clnt.PutObject(t.Context(), "bucket", "object", r, int64(parts*partSize), PutObjectOptions{
		PartSize:              partSize,
		ConcurrentStreamParts: true,
		NumThreads:            workers,
	})
	if err == nil {
		t.Fatal("PutObject must fail when every part attempt is rejected")
	}
	if code := ToErrorResponse(err).Code; code != "SlowDown" {
		t.Errorf("error code = %q, want SlowDown", code)
	}
	if aborts.Load() != 1 {
		t.Errorf("abort requests = %d, want 1", aborts.Load())
	}
	for n, got := range attemptsByPart {
		if got > partUploadMaxAttempts {
			t.Errorf("part %d attempts = %d, want at most %d", n, got, partUploadMaxAttempts)
		}
	}
}

// TestPartUploadPermanentErrorNotRetried proves non-retryable failures fail
// fast without consuming the retry budget or sending the part twice.
func TestPartUploadPermanentErrorNotRetried(t *testing.T) {
	const (
		partSize = 5 << 20
		workers  = 4
		parts    = 4
	)
	var mu sync.Mutex
	attemptsByPart := make(map[int]int)
	handler := func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Query().Has("uploads"):
			w.Write([]byte(`<InitiateMultipartUploadResult><UploadId>id</UploadId></InitiateMultipartUploadResult>`))
		case r.Method == http.MethodPut:
			n, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))
			io.Copy(io.Discard, r.Body)
			mu.Lock()
			attemptsByPart[n]++
			mu.Unlock()
			w.WriteHeader(http.StatusForbidden)
			io.WriteString(w, `<Error><Code>AccessDenied</Code><Message>access denied</Message></Error>`)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}
	clnt := newRetryTestClient(t, handler)

	r := bytes.NewReader(make([]byte, parts*partSize))
	_, err := clnt.PutObject(t.Context(), "bucket", "object", r, int64(parts*partSize), PutObjectOptions{
		PartSize:              partSize,
		ConcurrentStreamParts: true,
		NumThreads:            workers,
	})
	if err == nil {
		t.Fatal("PutObject must fail on AccessDenied")
	}
	if code := ToErrorResponse(err).Code; code != AccessDenied {
		t.Errorf("error code = %q, want AccessDenied", code)
	}
	for n, got := range attemptsByPart {
		if got != 1 {
			t.Errorf("part %d attempts = %d, want 1 (permanent error must not be retried)", n, got)
		}
	}
}
