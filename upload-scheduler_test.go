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
	"sync/atomic"
	"testing"
	"time"
)

// waitQueued waits until the scheduler has n pending waiters.
func waitQueued(t *testing.T, sched *uploadScheduler, n int) {
	t.Helper()
	for i := 0; i < 2000; i++ {
		sched.mu.Lock()
		total := 0
		for _, queue := range sched.queues {
			total += len(queue)
		}
		sched.mu.Unlock()
		if total >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d queued waiters", n)
}

func acquireAsync(s *uploadSession) chan struct{} {
	done := make(chan struct{})
	go func() {
		if err := s.acquire(context.Background()); err != nil {
			panic(err)
		}
		close(done)
	}()
	return done
}

func mustNotBeGranted(t *testing.T, ch chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s was granted a slot, want it to keep waiting", what)
	case <-time.After(50 * time.Millisecond):
	}
}

func mustBeGranted(t *testing.T, ch chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s was not granted a slot", what)
	}
}

// A session with many queued waiters must not monopolize the scheduler:
// waiters of different sessions are served in round-robin order.
func TestUploadSchedulerRoundRobin(t *testing.T) {
	sched := newUploadScheduler()
	holder := sched.begin(1)
	if err := holder.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	b := sched.begin(1)
	c := sched.begin(1)
	defer b.end()
	defer c.end()

	b1 := acquireAsync(b)
	waitQueued(t, sched, 1)
	b2 := acquireAsync(b)
	waitQueued(t, sched, 2)
	c1 := acquireAsync(c)
	waitQueued(t, sched, 3)

	holder.release()
	mustBeGranted(t, b1, "b's first waiter")
	mustNotBeGranted(t, c1, "c's waiter")

	// b still has a waiter queued, but the next slot must go to c.
	b.release()
	mustBeGranted(t, c1, "c's waiter")
	mustNotBeGranted(t, b2, "b's second waiter")

	c.release()
	mustBeGranted(t, b2, "b's second waiter")
	b.release()
}

// A session may hold up to the capacity it registered; beyond that it waits.
func TestUploadSchedulerCapacity(t *testing.T) {
	sched := newUploadScheduler()
	sess := sched.begin(3)
	defer sess.end()

	for i := 0; i < 3; i++ {
		if err := sess.acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	fourth := acquireAsync(sess)
	mustNotBeGranted(t, fourth, "fourth acquire over capacity 3")
	sess.release()
	mustBeGranted(t, fourth, "fourth acquire after a release")
	sess.release()
	sess.release()
	sess.release()
}

// When the largest session ends, the capacity shrinks to the largest
// remaining one.
func TestUploadSchedulerCapacityShrinksOnEnd(t *testing.T) {
	sched := newUploadScheduler()
	big := sched.begin(4)
	small := sched.begin(2)
	defer small.end()
	big.end()

	if err := small.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := small.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	third := acquireAsync(small)
	mustNotBeGranted(t, third, "third acquire over shrunk capacity 2")
	small.release()
	mustBeGranted(t, third, "third acquire after a release")
	small.release()
	small.release()
}

// A canceled acquire returns the context error and does not leak its slot.
func TestUploadSchedulerAcquireCancel(t *testing.T) {
	sched := newUploadScheduler()
	sess := sched.begin(1)
	defer sess.end()

	if err := sess.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sess.acquire(ctx); err != context.Canceled {
		t.Fatalf("error = %v, want %v", err, context.Canceled)
	}
	sess.release()
	// The slot is still there: the next acquire must not block.
	if err := sess.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	sess.release()
}

// Concurrent sessions share the capacity and never exceed it, and every
// session keeps making progress.
func TestUploadSchedulerConcurrentFairShare(t *testing.T) {
	sched := newUploadScheduler()
	const sessions = 3
	const rounds = 200

	var inUse, maxInUse atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < sessions; i++ {
		sess := sched.begin(2)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer sess.end()
			for j := 0; j < rounds; j++ {
				if err := sess.acquire(context.Background()); err != nil {
					t.Error(err)
					return
				}
				cur := inUse.Add(1)
				for {
					old := maxInUse.Load()
					if cur <= old || maxInUse.CompareAndSwap(old, cur) {
						break
					}
				}
				time.Sleep(100 * time.Microsecond)
				inUse.Add(-1)
				sess.release()
			}
		}()
	}
	wg.Wait()
	if got := maxInUse.Load(); got > 2 {
		t.Errorf("max slots in use = %d, want at most 2", got)
	}
}
