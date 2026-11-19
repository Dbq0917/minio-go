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
	"net/http"
	"time"
)

// Clock skew handling.
//
// Request signatures - and especially presigned URLs - are only valid
// within a time window anchored to the *server's* clock. When the local
// clock deviates from the server clock (e.g. across data centers without
// tight NTP sync), a presigned URL generated against the local clock is
// already partially - or fully - expired by the time the server sees it.
//
// To anchor signing to the server's time base, the client learns the
// offset between the local clock and the server clock from the Date
// header of every server response (see Client.do) and applies it to all
// signing times. Learning is passive: it adds no extra requests and no
// latency to signing. The offset can also be set explicitly with
// Client.SetClockOffset.

// timeNow is the local clock source. It is a variable so tests can
// simulate a skewed local clock.
var timeNow = time.Now

// now returns the current time on the server's time base: the local time
// corrected by the learned (or explicitly set) clock offset.
func (c *Client) now() time.Time {
	return timeNow().Add(time.Duration(c.clockOffset.Load())).UTC()
}

// updateClockOffset learns the offset between the local clock and the
// server clock from the Date header of a server response.
func (c *Client) updateClockOffset(header http.Header) {
	date := header.Get("Date")
	if date == "" {
		return
	}
	serverTime, err := time.Parse(http.TimeFormat, date)
	if err != nil {
		return
	}
	c.clockOffset.Store(int64(serverTime.Sub(timeNow())))
}

// SetClockOffset explicitly sets the offset between the local clock and
// the server clock. The offset is added to the local time when signing
// requests; pass a positive value when the local clock is behind the
// server clock. The offset is normally learned automatically from server
// responses, so this is only needed when the skew is known upfront and
// must be compensated before the first server response.
func (c *Client) SetClockOffset(offset time.Duration) {
	c.clockOffset.Store(int64(offset))
}

// ClockOffset returns the currently applied offset between the local
// clock and the server clock.
func (c *Client) ClockOffset() time.Duration {
	return time.Duration(c.clockOffset.Load())
}
