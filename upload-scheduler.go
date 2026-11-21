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
	"context"
	"sync"
)

// uploadScheduler bounds the number of part uploads a single client has in
// flight and shares them fairly between concurrent uploads.
//
// Without it every multipart upload runs NumThreads part uploads of its own,
// so N concurrent uploads drive N*NumThreads requests at the transport and
// exhaust its connection pool. The scheduler instead treats the largest
// NumThreads among the active uploads as the client's overall budget and
// hands the slots out round-robin: a lone upload still gets its full
// NumThreads worth of parallelism, while concurrent uploads each receive an
// equal share of the same budget and no upload can be starved by the others.
type uploadScheduler struct {
	mu sync.Mutex
	// inUse is the number of slots currently handed out, capacity the
	// number of slots the active sessions may hold in total.
	inUse    int
	capacity int
	// sessions holds every active upload. queues maps a session to its
	// pending acquire waiters, and order lists the sessions that have
	// waiters in round-robin order with rr the next session to serve.
	sessions map[*uploadSession]struct{}
	queues   map[*uploadSession][]chan struct{}
	order    []*uploadSession
	rr       int
}

// uploadSession is one upload's registration with the scheduler. It is not
// safe for concurrent use of end; acquire and release are.
type uploadSession struct {
	sched *uploadScheduler
	// slots is the maximum number of part uploads this upload runs at
	// once, i.e. its NumThreads.
	slots int
}

func newUploadScheduler() *uploadScheduler {
	return &uploadScheduler{
		sessions: make(map[*uploadSession]struct{}),
		queues:   make(map[*uploadSession][]chan struct{}),
	}
}

// partUploadSlots returns the scheduler that bounds this client's in-flight
// part uploads and shares them fairly between concurrent uploads.
func (c *Client) partUploadSlots() *uploadScheduler {
	c.slotsOnce.Do(func() {
		c.uploadSlots = newUploadScheduler()
	})
	return c.uploadSlots
}

// begin registers an upload that holds at most slots part-upload slots at
// once. The returned session must be closed with end once the upload's
// workers have stopped, and the upload's context must be canceled before
// then so no acquire of this session is still blocked.
func (s *uploadScheduler) begin(slots int) *uploadSession {
	if slots < 1 {
		slots = 1
	}
	sess := &uploadSession{sched: s, slots: slots}
	s.mu.Lock()
	s.sessions[sess] = struct{}{}
	if slots > s.capacity {
		s.capacity = slots
		s.grantLocked()
	}
	s.mu.Unlock()
	return sess
}

// end unregisters the session. Slots it still holds are returned by release
// as usual; the capacity shrinks to the largest remaining session.
func (s *uploadSession) end() {
	sched := s.sched
	sched.mu.Lock()
	delete(sched.sessions, s)
	capacity := 0
	for sess := range sched.sessions {
		if sess.slots > capacity {
			capacity = sess.slots
		}
	}
	sched.capacity = capacity
	sched.mu.Unlock()
}

// acquire blocks until the session is granted a part-upload slot or ctx is
// canceled. A granted slot must be returned with release.
func (s *uploadSession) acquire(ctx context.Context) error {
	sched := s.sched
	sched.mu.Lock()
	if sched.inUse < sched.capacity && len(sched.order) == 0 {
		sched.inUse++
		sched.mu.Unlock()
		return nil
	}
	ch := make(chan struct{})
	if _, ok := sched.queues[s]; !ok {
		sched.order = append(sched.order, s)
	}
	sched.queues[s] = append(sched.queues[s], ch)
	sched.mu.Unlock()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		sched.mu.Lock()
		if sched.removeWaiterLocked(s, ch) {
			sched.mu.Unlock()
			return ctx.Err()
		}
		// The slot was granted concurrently with the cancellation;
		// hand it back so it is not leaked.
		sched.mu.Unlock()
		s.release()
		return ctx.Err()
	}
}

// release returns a slot previously granted by acquire.
func (s *uploadSession) release() {
	sched := s.sched
	sched.mu.Lock()
	sched.inUse--
	sched.grantLocked()
	sched.mu.Unlock()
}

// grantLocked hands out slots to waiting sessions in round-robin order for
// as long as the capacity allows. Callers must hold s.mu.
func (s *uploadScheduler) grantLocked() {
	for s.inUse < s.capacity && len(s.order) > 0 {
		if s.rr >= len(s.order) {
			s.rr = 0
		}
		sess := s.order[s.rr]
		queue := s.queues[sess]
		ch := queue[0]
		queue = queue[1:]
		if len(queue) == 0 {
			delete(s.queues, sess)
			s.order = append(s.order[:s.rr], s.order[s.rr+1:]...)
			// rr now points at the next session in line.
		} else {
			s.queues[sess] = queue
			s.rr++
		}
		s.inUse++
		close(ch)
	}
}

// removeWaiterLocked removes ch from the session's queue, returning false if
// the slot was already granted. Callers must hold s.mu.
func (s *uploadScheduler) removeWaiterLocked(sess *uploadSession, ch chan struct{}) bool {
	queue, ok := s.queues[sess]
	if !ok {
		return false
	}
	for i, waiter := range queue {
		if waiter != ch {
			continue
		}
		queue = append(queue[:i], queue[i+1:]...)
		if len(queue) == 0 {
			delete(s.queues, sess)
			for j, queued := range s.order {
				if queued == sess {
					s.order = append(s.order[:j], s.order[j+1:]...)
					if j < s.rr {
						s.rr--
					}
					break
				}
			}
		} else {
			s.queues[sess] = queue
		}
		return true
	}
	return false
}
