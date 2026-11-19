/*
 * MinIO Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2015-2026 MinIO, Inc.
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
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"
)

// stubLocalClock replaces the local clock source with one that is offset
// by the given duration, simulating a machine whose clock is out of sync
// with the server. It returns a function restoring the real clock.
func stubLocalClock(offset time.Duration) (restore func()) {
	realTimeNow := timeNow
	timeNow = func() time.Time { return realTimeNow().Add(offset) }
	return func() { timeNow = realTimeNow }
}

func newClockSkewTestClient(t *testing.T, endpoint string) *Client {
	t.Helper()
	c, err := New(endpoint, &Options{
		Creds:  credentials.NewStaticV4("ak", "sk", ""),
		Secure: false,
		Region: "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// presignDate extracts the X-Amz-Date signing time of a presigned URL.
func presignDate(t *testing.T, u *url.URL) time.Time {
	t.Helper()
	signTime, err := time.Parse(iso8601DateFormat, u.Query().Get("X-Amz-Date"))
	if err != nil {
		t.Fatalf("cannot parse X-Amz-Date of %s: %v", u, err)
	}
	return signTime
}

// TestPresignedURLUsesServerTimeBase verifies that the signing time of a
// presigned URL is anchored to the server clock (local time + clock
// offset) instead of the raw local clock, while the requested expiry is
// left untouched.
func TestPresignedURLUsesServerTimeBase(t *testing.T) {
	// Local clock is 5 minutes behind the server.
	defer stubLocalClock(-5 * time.Minute)()

	c := newClockSkewTestClient(t, "localhost:9000")

	// Without any known offset the signing time follows the (slow)
	// local clock.
	u, err := c.PresignedPutObject(context.Background(), "bucket", "object", 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if skew := time.Since(presignDate(t, u)); skew < 4*time.Minute {
		t.Fatalf("expected signing time to follow the slow local clock, skew was %v", skew)
	}

	// Once the server clock offset is known, the signing time is
	// anchored to the server clock.
	c.SetClockOffset(5 * time.Minute)
	u, err = c.PresignedPutObject(context.Background(), "bucket", "object", 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if skew := time.Since(presignDate(t, u)); skew < -2*time.Second || skew > 2*time.Second {
		t.Fatalf("expected signing time anchored to the server clock, skew was %v", skew)
	}
	// The expiry itself must not be inflated to compensate for skew.
	if expires := u.Query().Get("X-Amz-Expires"); expires != "300" {
		t.Fatalf("expected X-Amz-Expires to stay 300, got %q", expires)
	}
}

// TestUpdateClockOffset verifies learning the server clock offset from
// the Date response header.
func TestUpdateClockOffset(t *testing.T) {
	localNow := time.Now().UTC().Truncate(time.Second)
	defer stubLocalClock(0)()
	timeNow = func() time.Time { return localNow }

	c := newClockSkewTestClient(t, "localhost:9000")

	// Server clock is 5 minutes ahead of the local clock.
	header := http.Header{}
	header.Set("Date", localNow.Add(5*time.Minute).Format(http.TimeFormat))
	c.updateClockOffset(header)
	if offset := c.ClockOffset(); offset < 4*time.Minute+58*time.Second || offset > 5*time.Minute {
		t.Fatalf("expected learned offset of about 5m, got %v", offset)
	}

	// A missing or unparsable Date header leaves the offset untouched.
	c.updateClockOffset(http.Header{})
	c.updateClockOffset(http.Header{"Date": []string{"not-a-date"}})
	if offset := c.ClockOffset(); offset < 4*time.Minute+58*time.Second || offset > 5*time.Minute {
		t.Fatalf("expected offset to remain about 5m, got %v", offset)
	}
}

// TestPresignedPutObjectWithSkewedClock simulates a client machine whose
// clock is 5 minutes behind the server and verifies that a presigned PUT
// URL is accepted by the server once the client has learned the server
// clock offset from a previous response.
func TestPresignedPutObjectWithSkewedClock(t *testing.T) {
	// The server validates presigned PUT URLs against its own clock.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			signTime, err := time.Parse(iso8601DateFormat, r.URL.Query().Get("X-Amz-Date"))
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			expires, err := strconv.ParseInt(r.URL.Query().Get("X-Amz-Expires"), 10, 64)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if time.Since(signTime) > time.Duration(expires)*time.Second {
				w.WriteHeader(http.StatusForbidden)
				w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>AccessDenied</Code><Message>Request has expired</Message><Resource>/bucket/object</Resource><RequestId>REQ</RequestId></Error>`))
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Local clock is 5 minutes behind the server.
	defer stubLocalClock(-5 * time.Minute)()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	c := newClockSkewTestClient(t, u.Host)

	put := func(code int) {
		t.Helper()
		presignedURL, err := c.PresignedPutObject(context.Background(), "bucket", "object", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequest(http.MethodPut, presignedURL.String(), nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
		if resp.StatusCode != code {
			t.Fatalf("expected PUT status %d, got %d", code, resp.StatusCode)
		}
	}

	// The local clock is 5 minutes slow and no server response has been
	// seen yet: the URL is already expired from the server's point of
	// view and the upload is rejected.
	put(http.StatusForbidden)

	// Any server response teaches the client the clock offset.
	if _, err := c.BucketExists(context.Background(), "bucket"); err != nil {
		t.Fatal(err)
	}
	if offset := c.ClockOffset(); offset < 4*time.Minute {
		t.Fatalf("expected learned offset of about 5m, got %v", offset)
	}

	// The next presigned URL is anchored to the server clock and the
	// upload succeeds without inflating the expiry.
	put(http.StatusOK)
}

// TestRequestTimeTooSkewedRetried verifies that a request rejected with
// RequestTimeTooSkewed is retried with a signing time corrected by the
// learned clock offset.
func TestRequestTimeTooSkewedRetried(t *testing.T) {
	var putCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			putCalls++
			signTime, err := time.Parse(iso8601DateFormat, r.Header.Get("X-Amz-Date"))
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			// The server tolerates at most 1 minute of clock skew.
			if skew := time.Since(signTime); skew > time.Minute || skew < -time.Minute {
				w.WriteHeader(http.StatusForbidden)
				w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>RequestTimeTooSkewed</Code><Message>The difference between the request time and the server's time is too large.</Message><Resource>/bucket/object</Resource><RequestId>REQ</RequestId></Error>`))
				return
			}
			w.Header().Set("ETag", "\"5eb63bbbe01eeed093cb22bb8f5acdc3\"")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Local clock is 5 minutes behind the server.
	defer stubLocalClock(-5 * time.Minute)()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	c := newClockSkewTestClient(t, u.Host)

	data := []byte("hello world")
	_, err = c.PutObject(context.Background(), "bucket", "object", bytes.NewReader(data), int64(len(data)), PutObjectOptions{})
	if err != nil {
		t.Fatalf("expected PutObject to succeed after clock skew correction, got: %v", err)
	}
	if putCalls != 2 {
		t.Fatalf("expected 2 PUT attempts (stale, then corrected), got %d", putCalls)
	}
}
