/*
 * MinIO Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2015-2017 MinIO, Inc.
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

package signer

import (
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Tests that the WithTime presign variants anchor the signature to the
// provided time instead of the local clock.
func TestPreSignWithTime(t *testing.T) {
	signTime := time.Date(2026, time.November, 20, 10, 0, 0, 0, time.UTC)

	req, err := http.NewRequest(http.MethodPut, "https://s3.amazonaws.com/bucket/object", nil)
	if err != nil {
		t.Fatal("Error:", err)
	}
	req = PreSignV4WithTime(*req, "ACCESS-KEY", "SECRET-KEY", "", "us-east-1", 300, signTime)
	query := req.URL.Query()
	if got := query.Get("X-Amz-Date"); got != "20261120T100000Z" {
		t.Fatalf("Error: expected X-Amz-Date 20261120T100000Z, got %q", got)
	}
	if got := query.Get("X-Amz-Expires"); got != "300" {
		t.Fatalf("Error: expected X-Amz-Expires 300, got %q", got)
	}
	if !strings.Contains(req.URL.RawQuery, "X-Amz-Signature=") {
		t.Fatal("Error: expected X-Amz-Signature query resource.")
	}

	req, err = http.NewRequest(http.MethodPut, "https://s3.amazonaws.com/bucket/object", nil)
	if err != nil {
		t.Fatal("Error:", err)
	}
	req = PreSignV2WithTime(*req, "ACCESS-KEY", "SECRET-KEY", 300, false, signTime)
	if got := req.URL.Query().Get("Expires"); got != strconv.FormatInt(signTime.Unix()+300, 10) {
		t.Fatalf("Error: expected Expires %d, got %q", signTime.Unix()+300, got)
	}
	if !strings.Contains(req.URL.RawQuery, "Signature=") {
		t.Fatal("Error: expected Signature query resource.")
	}
}

// Tests that SignV4TrailerWithTime stamps the request with the provided
// signing time.
func TestSignV4TrailerWithTime(t *testing.T) {
	signTime := time.Date(2026, time.November, 20, 10, 0, 0, 0, time.UTC)

	req, err := http.NewRequest(http.MethodPut, "https://s3.amazonaws.com/bucket/object", nil)
	if err != nil {
		t.Fatal("Error:", err)
	}
	req = SignV4TrailerWithTime(*req, "ACCESS-KEY", "SECRET-KEY", "", "us-east-1", ServiceTypeS3, nil, signTime)
	if got := req.Header.Get("X-Amz-Date"); got != "20261120T100000Z" {
		t.Fatalf("Error: expected X-Amz-Date 20261120T100000Z, got %q", got)
	}
	if req.Header.Get("Authorization") == "" {
		t.Fatal("Error: expected Authorization header.")
	}
}

// Tests signature calculation.
func TestSignatureCalculationV4(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://s3.amazonaws.com", nil)
	if err != nil {
		t.Fatal("Error:", err)
	}
	req = SignV4(*req, "", "", "", "us-east-1")
	if req.Header.Get("Authorization") != "" {
		t.Fatal("Error: anonymous credentials should not have Authorization header.")
	}

	req = PreSignV4(*req, "", "", "", "us-east-1", 0)
	if strings.Contains(req.URL.RawQuery, "X-Amz-Signature") {
		t.Fatal("Error: anonymous credentials should not have Signature query resource.")
	}

	req = SignV4(*req, "ACCESS-KEY", "SECRET-KEY", "", "us-east-1")
	if req.Header.Get("Authorization") == "" {
		t.Fatal("Error: normal credentials should have Authorization header.")
	}

	req = PreSignV4(*req, "ACCESS-KEY", "SECRET-KEY", "", "us-east-1", 0)
	if !strings.Contains(req.URL.RawQuery, "X-Amz-Signature") {
		t.Fatal("Error: normal credentials should have Signature query resource.")
	}
}

func TestSignatureCalculationV2(t *testing.T) {
	testCases := []struct {
		endpointURL string
		virtualHost bool
	}{
		{endpointURL: "https://s3.amazonaws.com/", virtualHost: false},
		{endpointURL: "https://testbucket.s3.amazonaws.com/", virtualHost: true},
	}

	for i, testCase := range testCases {
		req, err := http.NewRequest(http.MethodGet, testCase.endpointURL, nil)
		if err != nil {
			t.Fatalf("Test %d, Error: %v", i+1, err)
		}

		req = SignV2(*req, "", "", testCase.virtualHost)
		if req.Header.Get("Authorization") != "" {
			t.Fatalf("Test %d, Error: anonymous credentials should not have Authorization header.", i+1)
		}

		req = PreSignV2(*req, "", "", 0, testCase.virtualHost)
		if strings.Contains(req.URL.RawQuery, "Signature") {
			t.Fatalf("Test %d, Error: anonymous credentials should not have Signature query resource.", i+1)
		}

		req = SignV2(*req, "ACCESS-KEY", "SECRET-KEY", testCase.virtualHost)
		if req.Header.Get("Authorization") == "" {
			t.Fatalf("Test %d, Error: normal credentials should have Authorization header.", i+1)
		}

		req = PreSignV2(*req, "ACCESS-KEY", "SECRET-KEY", 0, testCase.virtualHost)
		if !strings.Contains(req.URL.RawQuery, "Signature") {
			t.Fatalf("Test %d, Error: normal credentials should not have Signature query resource.", i+1)
		}
	}
}
