// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import "sync"

// This file contains the per-follower replication flow-control window. The
// window counts log entries (not RPC requests), so a batch cannot bypass the
// limit by carrying many entries in one RPC. All state is local to one
// followerReplication and is reset whenever its transport session ends.

// initReplicationWindow initializes the per-follower flow-control window. A
// zero limit selects DefaultMaxInflightLogs.
func (s *followerReplication) initReplicationWindow(maxInflight uint64) {
	s.windowLock.Lock()
	defer s.windowLock.Unlock()

	if maxInflight == 0 {
		maxInflight = DefaultMaxInflightLogs
	}
	s.windowSize = maxInflight
	if s.windowCond == nil {
		s.windowCond = sync.NewCond(&s.windowLock)
	}
	if s.abortCh == nil {
		s.abortCh = make(chan struct{})
	}
}

// inflightCount returns the number of log entries currently outstanding.
func (s *followerReplication) inflightCount() uint64 {
	s.windowLock.Lock()
	defer s.windowLock.Unlock()
	return s.inflight
}

// maxWindow returns the configured per-follower entry cap.
func (s *followerReplication) maxWindow() uint64 {
	s.windowLock.Lock()
	defer s.windowLock.Unlock()
	return s.windowSize
}

// acquireInflight reserves n entries, blocking until the window has room. It
// returns false if abort is closed while waiting. A zero windowSize disables
// flow control and preserves the historical unbounded behavior for manually
// constructed followerReplication values in unit tests.
func (s *followerReplication) acquireInflight(n int, abort <-chan struct{}) bool {
	if n <= 0 {
		return true
	}
	add := uint64(n)

	s.windowLock.Lock()
	defer s.windowLock.Unlock()

	if s.windowCond == nil {
		s.windowCond = sync.NewCond(&s.windowLock)
	}
	for s.windowSize > 0 && s.inflight+add > s.windowSize {
		if waitForWindow(s.windowCond, abort) {
			return false
		}
	}
	s.inflight += add
	return true
}

// waitForWindow parks on cond until Broadcast or the abort channel closes.
// cond's lock must be held by the caller. The return value is true when abort
// fired.
func waitForWindow(cond *sync.Cond, abort <-chan struct{}) (aborted bool) {
	if cond == nil {
		// A nil abort channel blocks forever, which is the correct fallback
		// for zero-value test structs that never initialize a condition.
		if abort == nil {
			return false
		}
		<-abort
		return true
	}

	wakeWatcher := make(chan struct{})
	go func() {
		select {
		case <-abort:
			// Acquire the condition lock before broadcasting. cond.Wait
			// releases that lock atomically when it parks, so this cannot
			// broadcast before a waiter is registered and lose the wake-up.
			cond.L.Lock()
			cond.Broadcast()
			cond.L.Unlock()
		case <-wakeWatcher:
		}
	}()

	cond.Wait()
	close(wakeWatcher)

	select {
	case <-abort:
		return true
	default:
		return false
	}
}

// waitForWindowCapacity blocks until at least n entries fit without exceeding
// windowSize. It is used before constructing a batch so callers can shrink the
// batch instead of deadlocking when MaxAppendEntries is larger than the window.
func (s *followerReplication) waitForWindowCapacity(n uint64, abort <-chan struct{}) bool {
	if n == 0 {
		return true
	}

	s.windowLock.Lock()
	defer s.windowLock.Unlock()

	if s.windowCond == nil {
		s.windowCond = sync.NewCond(&s.windowLock)
	}
	for s.windowSize > 0 && s.inflight+n > s.windowSize {
		if waitForWindow(s.windowCond, abort) {
			return false
		}
	}
	return true
}

// appendEntriesLimit returns the largest batch size that can be sent now. It
// caps the configured MaxAppendEntries by the window's currently available
// capacity. If no capacity is available it returns zero; callers should wait
// before building a non-heartbeat batch.
func (s *followerReplication) appendEntriesLimit(configured int) int {
	if configured <= 0 {
		configured = 1
	}

	s.windowLock.Lock()
	defer s.windowLock.Unlock()

	if s.windowSize == 0 {
		return configured
	}
	available := s.windowSize
	if s.inflight < s.windowSize {
		available = s.windowSize - s.inflight
	} else {
		available = 0
	}
	if uint64(configured) < available {
		return configured
	}
	return int(min(available, uint64(configured)))
}

// releaseInflight settles n outstanding entries and wakes one/many senders
// waiting for capacity. It clamps at zero because teardown can release a
// batch after resetInflight already discarded the session's counters.
func (s *followerReplication) releaseInflight(n int) {
	if n <= 0 {
		return
	}

	s.windowLock.Lock()
	if uint64(n) >= s.inflight {
		s.inflight = 0
	} else {
		s.inflight -= uint64(n)
	}
	if s.windowCond != nil {
		s.windowCond.Broadcast()
	}
	s.windowLock.Unlock()
}

// resetInflight releases every outstanding slot. It is called when a transport
// session ends (disconnect, pipeline failure, step-down, removal). Unacknowledged
// logs are not discarded from the Raft log; they are resent from the monotonic
// nextIndex after reconnect/fallback.
func (s *followerReplication) resetInflight() {
	s.windowLock.Lock()
	s.inflight = 0
	if s.windowCond != nil {
		s.windowCond.Broadcast()
	}
	s.windowLock.Unlock()
}
