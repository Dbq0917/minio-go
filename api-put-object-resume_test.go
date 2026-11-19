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
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"testing"
)

func listPartsXML(bucket, object, uploadID string, parts []ObjectPart) string {
	var b bytes.Buffer
	b.WriteString(`<ListPartsResult><Bucket>` + bucket + `</Bucket><Key>` + object + `</Key><UploadId>` + uploadID + `</UploadId>`)
	b.WriteString(`<PartNumberMarker>0</PartNumberMarker><NextPartNumberMarker>` + strconv.Itoa(len(parts)) + `</NextPartNumberMarker>`)
	b.WriteString(`<MaxParts>1000</MaxParts><IsTruncated>false</IsTruncated>`)
	for _, part := range parts {
		b.WriteString(`<Part><PartNumber>` + strconv.Itoa(part.PartNumber) + `</PartNumber>`)
		b.WriteString(`<ETag>"` + part.ETag + `"</ETag><Size>` + strconv.FormatInt(part.Size, 10) + `</Size></Part>`)
	}
	b.WriteString(`</ListPartsResult>`)
	return b.String()
}

func TestPutObjectMultipartResumeUsesRecordedPartSize(t *testing.T) {
	const (
		recordedPartSize = 5 << 20
		partCount        = 3
		totalSize        = partCount*recordedPartSize + recordedPartSize/2
	)

	// The default layout for this object would use 16 MiB parts. The stored
	// upload uses 5 MiB parts, so resuming with the computed size would send
	// different boundaries and produce an InvalidPart at completion.
	if _, computed, _, err := OptimalPartInfo(totalSize, 0); err != nil {
		t.Fatal(err)
	} else if computed == recordedPartSize {
		t.Fatalf("test object must make the computed part size differ from the recorded one")
	}

	data := make([]byte, totalSize)
	for part := 1; part <= partCount+1; part++ {
		start := int64(part-1) * recordedPartSize
		if start >= totalSize {
			break
		}
		end := min(start+recordedPartSize, int64(totalSize))
		for i := start; i < end; i++ {
			data[i] = byte(part)
		}
	}

	type putRequest struct {
		partNumber int
		body       []byte
	}
	var (
		mu        sync.Mutex
		puts      []putRequest
		completes int
		initiates int
		complete  completeMultipartUpload
	)

	clnt := newTestStatClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Query().Has("uploads"):
			initiates++
			w.Write([]byte(`<InitiateMultipartUploadResult><UploadId>new-id</UploadId></InitiateMultipartUploadResult>`))
		case r.Method == http.MethodGet:
			w.Write([]byte(listPartsXML("bucket", "object", "resume-id", []ObjectPart{
				{PartNumber: 1, ETag: "etag1", Size: recordedPartSize},
				{PartNumber: 2, ETag: "etag2", Size: recordedPartSize},
			})))
		case r.Method == http.MethodPut:
			n, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			puts = append(puts, putRequest{partNumber: n, body: body})
			mu.Unlock()
			w.Header().Set("ETag", `"etag`+strconv.Itoa(n)+`"`)
		case r.Method == http.MethodPost:
			mu.Lock()
			completes++
			_ = xml.NewDecoder(r.Body).Decode(&complete)
			mu.Unlock()
			w.Write([]byte(`<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>object</Key><ETag>"done"</ETag></CompleteMultipartUploadResult>`))
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	})

	info, err := clnt.PutObject(t.Context(), "bucket", "object", bytes.NewReader(data), totalSize, PutObjectOptions{
		UploadID:             "resume-id",
		DisableContentSha256: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != totalSize {
		t.Errorf("Size = %d, want %d", info.Size, totalSize)
	}

	mu.Lock()
	defer mu.Unlock()
	if initiates != 0 {
		t.Errorf("initiate calls = %d, want 0: resuming must not start a new upload", initiates)
	}
	if completes != 1 {
		t.Errorf("complete calls = %d, want 1", completes)
	}
	if len(puts) != 2 {
		t.Fatalf("uploaded parts = %d, want 2: only parts 3 and 4 are missing", len(puts))
	}
	wantParts := []struct {
		number int
		size   int
	}{
		{3, recordedPartSize},
		{4, recordedPartSize / 2},
	}
	for i, want := range wantParts {
		got := puts[i]
		if got.partNumber != want.number {
			t.Errorf("PUT %d part number = %d, want %d", i, got.partNumber, want.number)
		}
		if len(got.body) != want.size {
			t.Errorf("PUT part %d size = %d, want %d", got.partNumber, len(got.body), want.size)
		}
		start := int64(want.number-1) * recordedPartSize
		if !bytes.Equal(got.body, data[start:start+int64(want.size)]) {
			t.Errorf("PUT part %d did not read from the resumed offset", got.partNumber)
		}
	}

	if len(complete.Parts) != partCount+1 {
		t.Fatalf("completed parts = %d, want %d", len(complete.Parts), partCount+1)
	}
	for i, part := range complete.Parts {
		if part.PartNumber != i+1 {
			t.Errorf("completed part %d number = %d, want %d", i, part.PartNumber, i+1)
		}
	}
}

func TestPutObjectMultipartResumeRejectsInconsistentParts(t *testing.T) {
	const partSize = 5 << 20
	var puts int
	clnt := newTestStatClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet:
			w.Write([]byte(listPartsXML("bucket", "object", "resume-id", []ObjectPart{
				{PartNumber: 1, ETag: "etag1", Size: partSize},
				// A non-final part with a different size cannot belong to the
				// same fixed-size layout.
				{PartNumber: 2, ETag: "etag2", Size: partSize + 1},
			})))
		case r.Method == http.MethodPut:
			puts++
			w.Header().Set("ETag", `"etag"`)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	})

	_, err := clnt.PutObject(t.Context(), "bucket", "object", bytes.NewReader(make([]byte, 3*partSize)), 3*partSize, PutObjectOptions{
		UploadID: "resume-id",
	})
	if err == nil {
		t.Fatal("PutObject accepted an upload with inconsistent stored part sizes")
	}
	if code := ToErrorResponse(err).Code; code != InvalidArgument {
		t.Errorf("error code = %q, want %q (err: %v)", code, InvalidArgument, err)
	}
	if puts != 0 {
		t.Errorf("PUT requests = %d, want 0: invalid layout must fail before uploading", puts)
	}
}

func TestPutObjectMultipartResumeRequiresSeekableReader(t *testing.T) {
	clnt := newTestStatClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Write([]byte(listPartsXML("bucket", "object", "resume-id", []ObjectPart{
				{PartNumber: 1, ETag: "etag1", Size: 5 << 20},
			})))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	_, err := clnt.PutObject(t.Context(), "bucket", "object", io.LimitReader(bytes.NewReader(make([]byte, 10<<20)), 10<<20), 10<<20, PutObjectOptions{
		UploadID: "resume-id",
	})
	if code := ToErrorResponse(err).Code; code != InvalidArgument {
		t.Errorf("error code = %q, want %q (err: %v)", code, InvalidArgument, err)
	}
}

func TestResumePartLayoutRejectsUnsupportedSizes(t *testing.T) {
	layout := func(parts map[int]ObjectPart) error {
		_, _, _, err := UploadLimits{}.resumePartLayout(20<<20, 0, parts)
		return err
	}

	for _, tc := range []struct {
		name  string
		parts map[int]ObjectPart
	}{
		{
			name: "part below minimum",
			parts: map[int]ObjectPart{
				1: {PartNumber: 1, Size: 4 << 20},
			},
		},
		{
			name: "part above maximum",
			parts: map[int]ObjectPart{
				1: {PartNumber: 1, Size: defaultMaxPartSize + 1},
			},
		},
		{
			name: "stored part beyond layout",
			parts: map[int]ObjectPart{
				1: {PartNumber: 1, Size: 5 << 20},
				9: {PartNumber: 9, Size: 5 << 20},
			},
		},
		{
			name: "last part size mismatch",
			parts: map[int]ObjectPart{
				1: {PartNumber: 1, Size: 5 << 20},
				2: {PartNumber: 2, Size: 4 << 20},
			},
		},
		{
			name: "part one missing without explicit size",
			parts: map[int]ObjectPart{
				2: {PartNumber: 2, Size: 5 << 20},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := layout(tc.parts); err == nil {
				t.Fatal("resumePartLayout accepted an unsupported part layout")
			}
		})
	}
}

func TestResumePartLayoutAcceptsRecordedSize(t *testing.T) {
	const partSize = 5 << 20
	parts := map[int]ObjectPart{
		1: {PartNumber: 1, Size: partSize},
		2: {PartNumber: 2, Size: partSize},
	}
	total, gotPartSize, last, err := UploadLimits{}.resumePartLayout(3*partSize+1, 0, parts)
	if err != nil {
		t.Fatal(err)
	}
	if total != 4 || gotPartSize != partSize || last != 1 {
		t.Errorf("layout = (%d, %d, %d), want (4, %d, 1)", total, gotPartSize, last, partSize)
	}
}

func TestPutObjectMultipartResumeDoesNotAbortOnFailure(t *testing.T) {
	const partSize = 5 << 20
	var aborts int
	clnt := newTestStatClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet:
			w.Write([]byte(listPartsXML("bucket", "object", "resume-id", []ObjectPart{
				{PartNumber: 1, ETag: "etag1", Size: partSize},
			})))
		case r.Method == http.MethodPut:
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`<Error><Code>BadDigest</Code><Message>bad part</Message></Error>`))
		case r.Method == http.MethodDelete:
			aborts++
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	})

	_, err := clnt.PutObject(t.Context(), "bucket", "object", bytes.NewReader(make([]byte, 2*partSize)), 2*partSize, PutObjectOptions{
		UploadID: "resume-id",
	})
	if err == nil {
		t.Fatal("PutObject succeeded despite a failing part upload")
	}
	if aborts != 0 {
		t.Errorf("abort calls = %d, want 0: a failed resume must keep the stored parts", aborts)
	}
}

func TestPutObjectMultipartResumeUnknownSize(t *testing.T) {
	clnt := newTestStatClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request %s %s", r.Method, r.URL)
	})

	_, err := clnt.PutObject(t.Context(), "bucket", "object", bytes.NewReader(nil), -1, PutObjectOptions{
		UploadID: "resume-id",
	})
	if code := ToErrorResponse(err).Code; code != InvalidArgument {
		t.Errorf("error code = %q, want %q (err: %v)", code, InvalidArgument, err)
	}
}

func TestResumePartLayoutErrorIncludesPart(t *testing.T) {
	_, _, _, err := UploadLimits{}.resumePartLayout(20<<20, 0, map[int]ObjectPart{
		1: {PartNumber: 1, Size: 5 << 20},
		2: {PartNumber: 2, Size: 5<<20 + 1},
	})
	if err == nil || !bytes.Contains([]byte(fmt.Sprint(err)), []byte("part 2")) {
		t.Fatalf("error should identify the inconsistent part, got %v", err)
	}
}
