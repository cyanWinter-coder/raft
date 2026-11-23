// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"errors"
	"sync"

	"time"
)

const (
	// maxPipelineInflight bounds the number of AppendEntries batches a single
	// pipeline session may have in flight at the same time. The bound lives in
	// the replication layer so it applies to every Transport implementation
	// instead of relying on transport-internal buffering.
	maxPipelineInflight = 128

	// pipelineDrainTimeout bounds how long a closing session waits for
	// responses that were already started. Transports normally deliver (or
	// error) every in-flight future well before this, but the bound keeps a
	// broken transport from wedging the replication goroutine forever.
	pipelineDrainTimeout = 2 * time.Second
)

var (
	// errPipelineBatchRejected marks the unfinished batches of a session whose
	// follower rejected an AppendEntries batch. The caller falls back to
	// standard (non-pipelined) replication.
	errPipelineBatchRejected = errors.New("append entries batch rejected by follower")

	// errPipelineStaleTerm marks the unfinished batches of a session that
	// observed a response carrying a newer term.
	errPipelineStaleTerm = errors.New("pipeline observed a newer term")

	// errPipelineRPCFailed marks the unfinished batches of a session whose
	// transport reported an RPC error.
	errPipelineRPCFailed = errors.New("pipeline append entries RPC failed")
)

// pipelineOutcome classifies the result of processing one pipelined response.
type pipelineOutcome int

const (
	// pipelineOutcomeAdvanced means the response was successful and the
	// replicated position advanced over one or more contiguous batches.
	pipelineOutcomeAdvanced pipelineOutcome = iota

	// pipelineOutcomeGap means the response was successful but an earlier
	// batch is still outstanding, so the replicated position must not move.
	pipelineOutcomeGap

	// pipelineOutcomeRejected means the follower rejected the batch; the
	// session has failed all unfinished batches and replication must fall
	// back to standard mode.
	pipelineOutcomeRejected

	// pipelineOutcomeStaleTerm means a response carried a newer term; the
	// session has failed all unfinished batches and the leader must step
	// down.
	pipelineOutcomeStaleTerm

	// pipelineOutcomeRPCError means the transport returned an error for the
	// batch; the session has failed all unfinished batches and replication
	// must fall back to standard mode.
	pipelineOutcomeRPCError

	// pipelineOutcomeStale means the response belongs to an older generation
	// or an already finalized batch, so it was only allowed to complete its
	// own waiter and must not change any session state.
	pipelineOutcomeStale
)

// pipelineBatchResult is the value returned by pipelineSession.handle. It
// carries everything the raft replication goroutine needs to perform its side
// effects outside the session lock.
type pipelineBatchResult struct {
	outcome pipelineOutcome

	// matchIndex is set when outcome == pipelineOutcomeAdvanced: the highest
	// log index now known to be replicated without a hole.
	matchIndex uint64

	// frontier is the highest index known to be replicated without a hole at
	// the time the batch was handled. It is set for every outcome so callers
	// can safely reconcile nextIndex without trusting a hint that would skip
	// an unacknowledged gap.
	frontier uint64

	// lastLog and noRetryBackoff are populated when outcome ==
	// pipelineOutcomeRejected and mirror AppendEntriesResponse so the raft
	// replication loop can reconcile nextIndex exactly like standard mode.
	lastLog        uint64
	noRetryBackoff bool

	// err is set for the failure outcomes.
	err error
}

// pipelineBatch tracks a single AppendEntries request sent inside a
// pipelineSession. A batch is finalized exactly once: either its own response
// completes it successfully, or a terminal session event (RPC failure,
// rejection, newer term, close) fails it together with every other unfinished
// batch.
type pipelineBatch struct {
	// gen is the pipeline generation (connection session id) the batch was
	// sent under.
	gen uint64

	req  *AppendEntriesRequest
	resp *AppendEntriesResponse

	start time.Time

	// finished is guarded by pipelineSession.lock. Once set, the batch has
	// released its in-flight slot and must never be finalized again.
	finished bool
	// succeeded is guarded by pipelineSession.lock and only meaningful once
	// finished is set.
	succeeded bool
	// err records why a failed batch ended. It is informational; the session
	// never resends or re-fails a batch.
	err error
}

// pipelineSession owns the pipeline used for a single connection session and
// every batch sent through it. It enforces:
//
//   - generation isolation: responses only ever touch the session that sent
//     them, and batches of an older generation cannot update newer state;
//   - contiguous advancement: the replicated position only moves over
//     successful batches in send order, so an out-of-order success cannot
//     jump over a gap;
//   - single completion: every unfinished batch is failed exactly once on any
//     terminal event, never twice and never silently dropped;
//   - bounded concurrency: send acquires a slot before touching the transport;
//   - drain on close: a closed session rejects new batches while already
//     started responses keep being consumed until the in-flight set is empty.
type pipelineSession struct {
	gen      uint64
	pipeline AppendPipeline

	// inflight is the bounded-concurrency semaphore acquired by send and
	// released when a batch is finalized.
	inflight chan struct{}

	// shutdownCh is closed once the session stops accepting new batches.
	// Sends blocked on a full in-flight set unblock through it.
	shutdownCh chan struct{}

	lock sync.Mutex

	// closed is set when no new batch may be accepted.
	closed bool
	// finalized is set once every batch still tracked has been failed. It is
	// separate from closed because stopping sends during a drain leaves the
	// in-flight batches pending.
	finalized bool

	// batches indexes every unfinished batch by the request pointer, which is
	// the same pointer AppendFuture.Request() returns for its response.
	batches map[*AppendEntriesRequest]*pipelineBatch

	// queue preserves send order and is consumed from the head as soon as
	// contiguous head batches complete successfully.
	queue []*pipelineBatch

	// replicatedIndex is the highest log index known to be replicated without
	// a hole.
	replicatedIndex uint64

	// terminal is the outcome that ended the session (or 0 if it ended only
	// because the caller stopped it). It is recorded by handle so the
	// replication goroutine can apply backoff and fallback decisions after the
	// decoder has been joined, instead of mutating replication-only fields
	// from the decoder goroutine. Guarded by lock.
	terminal    pipelineOutcome
	terminalErr error
	terminalSet bool
	// lastLog and noRetry capture the follower's hint from a rejected batch so
	// pipelineReplicate can reconcile nextIndex after the decoder is joined.
	lastLog uint64
	noRetry bool

	// idleCh is closed once no batch and no in-flight slot remain. Because a
	// session that stopped accepting sends can only lose work, this edge is
	// permanent and is the signal waitEmpty blocks on.
	idleCh     chan struct{}
	idleClosed bool

	// transportClosed guards the single close of the underlying pipeline.
	transportClosed bool
}

// newPipelineSession wraps pipeline as a new generation session whose
// replicated frontier starts at zero.
func newPipelineSession(gen uint64, pipeline AppendPipeline) *pipelineSession {
	return newPipelineSessionAt(gen, pipeline, 0)
}

// newPipelineSessionAt wraps pipeline as a new generation session that starts
// with the given replicated frontier (the last index known to be durable on
// the follower before this connection session began).
func newPipelineSessionAt(gen uint64, pipeline AppendPipeline, frontier uint64) *pipelineSession {
	return &pipelineSession{
		gen:             gen,
		pipeline:        pipeline,
		inflight:        make(chan struct{}, maxPipelineInflight),
		shutdownCh:      make(chan struct{}),
		batches:         make(map[*AppendEntriesRequest]*pipelineBatch),
		replicatedIndex: frontier,
		idleCh:          make(chan struct{}),
	}
}

// consumer returns the transport pipeline's response channel.
func (s *pipelineSession) consumer() <-chan AppendFuture {
	return s.pipeline.Consumer()
}

// frontier returns the highest index known to be replicated without a hole.
func (s *pipelineSession) frontier() uint64 {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.replicatedIndex
}

// send enqueues a new AppendEntries batch in the session. It blocks while the
// bounded in-flight limit is reached and returns ErrPipelineShutdown once the
// session has stopped accepting batches. On success the returned batch is
// tracked until its response is handled or the session terminates.
func (s *pipelineSession) send(req *AppendEntriesRequest) (*pipelineBatch, error) {
	// Bound concurrency without holding the session lock: a blocked send must
	// not prevent handle or close from making progress.
	select {
	case s.inflight <- struct{}{}:
	case <-s.shutdownCh:
		return nil, ErrPipelineShutdown
	}

	s.lock.Lock()
	if s.closed {
		s.releaseSlotLocked()
		s.signalIdleLocked()
		s.lock.Unlock()
		return nil, ErrPipelineShutdown
	}

	batch := &pipelineBatch{
		gen:   s.gen,
		req:   req,
		resp:  new(AppendEntriesResponse),
		start: time.Now(),
	}
	s.batches[req] = batch
	s.queue = append(s.queue, batch)
	s.lock.Unlock()

	if _, err := s.pipeline.AppendEntries(req, batch.resp); err != nil {
		// The transport refused the batch (for example the connection was
		// closed underneath us). Finalize this batch and every other
		// outstanding one, and abort the transport so a writer blocked on a
		// full pipeline is released.
		s.lock.Lock()
		s.failAllLocked(err)
		s.lock.Unlock()
		s.closeTransport()
		return nil, err
	}
	return batch, nil
}

// handle processes one response future delivered by the transport pipeline.
// Responses may arrive out of order: only contiguous successful batches
// advance the replicated position. handle itself only mutates session state;
// the returned result describes the side effects the caller must apply.
func (s *pipelineSession) handle(future AppendFuture) pipelineBatchResult {
	req := future.Request()

	// Only claim a response this session is still waiting for. A response
	// from an older generation, or one whose batch was already finalized by a
	// terminal event, is ignored without consulting the future: the caller's
	// own waiter is already complete and current state must not move.
	s.lock.Lock()
	batch, ok := s.batches[req]
	if !ok || batch.finished || batch.gen != s.gen {
		frontier := s.replicatedIndex
		s.lock.Unlock()
		return pipelineBatchResult{outcome: pipelineOutcomeStale, frontier: frontier}
	}
	s.lock.Unlock()

	// A future sitting on the consumer channel has already been responded to
	// by the transport, so these calls do not block.
	rpcErr := future.Error()
	if rpcErr != nil {
		s.lock.Lock()
		defer s.lock.Unlock()
		frontier := s.replicatedIndex
		if batch.finished {
			return pipelineBatchResult{outcome: pipelineOutcomeStale, frontier: frontier}
		}
		s.recordTerminalLocked(pipelineOutcomeRPCError, rpcErr)
		s.failAllLocked(errPipelineRPCFailed)
		return pipelineBatchResult{outcome: pipelineOutcomeRPCError, frontier: frontier, err: rpcErr}
	}
	resp := future.Response()

	s.lock.Lock()
	defer s.lock.Unlock()
	frontierBefore := s.replicatedIndex

	// The batch may have been finalized while we were outside the lock (for
	// example a concurrent close). In that case this late response must not
	// perform any success side effect.
	if batch.finished {
		return pipelineBatchResult{outcome: pipelineOutcomeStale, frontier: frontierBefore}
	}

	// A newer term ends the session. Every unfinished batch fails once.
	if resp.Term > req.Term {
		s.recordTerminalLocked(pipelineOutcomeStaleTerm, errPipelineStaleTerm)
		s.failAllLocked(errPipelineStaleTerm)
		return pipelineBatchResult{outcome: pipelineOutcomeStaleTerm, frontier: frontierBefore, err: errPipelineStaleTerm}
	}

	// A rejected batch creates a gap: fail the rejected batch and everything
	// still outstanding, so no later success can advance over the hole.
	if !resp.Success {
		s.recordTerminalLocked(pipelineOutcomeRejected, errPipelineBatchRejected)
		s.lastLog = resp.LastLog
		s.noRetry = resp.NoRetryBackoff
		s.failAllLocked(errPipelineBatchRejected)
		return pipelineBatchResult{
			outcome:        pipelineOutcomeRejected,
			frontier:       frontierBefore,
			lastLog:        resp.LastLog,
			noRetryBackoff: resp.NoRetryBackoff,
			err:            errPipelineBatchRejected,
		}
	}

	// Successful response. Mark the batch done; whether the replicated
	// position may move depends on the batches ahead of it.
	batch.succeeded = true
	batch.finished = true
	delete(s.batches, req)
	s.releaseSlotLocked()

	advanced := s.advanceHeadLocked()
	s.signalIdleLocked()
	if advanced > frontierBefore {
		return pipelineBatchResult{outcome: pipelineOutcomeAdvanced, matchIndex: advanced, frontier: advanced}
	}

	// Either an earlier batch is still outstanding, or this batch carried no
	// new entries (for example an AppendEntries used to propagate the commit
	// index). In both cases the position must not move.
	return pipelineBatchResult{outcome: pipelineOutcomeGap, frontier: advanced}
}

// advanceHeadLocked pops every contiguous successful batch from the head of
// the queue and returns the highest index reached. A failed or still
// outstanding head batch stops advancement, however successful the batches
// behind it are. Must be called with the lock held.
func (s *pipelineSession) advanceHeadLocked() uint64 {
	for len(s.queue) > 0 {
		head := s.queue[0]
		if !head.finished || !head.succeeded {
			break
		}
		s.queue = s.queue[1:]
		if n := len(head.req.Entries); n > 0 {
			if last := head.req.Entries[n-1].Index; last > s.replicatedIndex {
				s.replicatedIndex = last
			}
		}
	}
	return s.replicatedIndex
}

// recordTerminal remembers the outcome that ended the session so the
// replication goroutine can apply its side effects after joining the decoder.
// The first terminal outcome wins; later stale responses cannot overwrite it.
func (s *pipelineSession) recordTerminal(outcome pipelineOutcome, err error) {
	s.lock.Lock()
	s.recordTerminalLocked(outcome, err)
	s.lock.Unlock()
}

// recordTerminalLocked is recordTerminal with the lock already held.
func (s *pipelineSession) recordTerminalLocked(outcome pipelineOutcome, err error) {
	if s.terminalSet {
		return
	}
	s.terminalSet = true
	s.terminal = outcome
	s.terminalErr = err
}

// terminalResult reports the outcome that ended the session, if any, together
// with the follower hint captured for a rejection and the terminal error. It
// is safe to call from the replication goroutine once the decoder has
// finished.
func (s *pipelineSession) terminalResult() (pipelineOutcome, bool, uint64, bool, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.terminal, s.terminalSet, s.lastLog, s.noRetry, s.terminalErr
}

// failAllLocked finalizes every unfinished batch exactly once and closes the
// session so no new batch can be accepted. Batches that already completed
// successfully have been removed from the map and are left untouched, so no
// batch is ever completed twice. Must be called with the lock held.
func (s *pipelineSession) failAllLocked(reason error) {
	for req, batch := range s.batches {
		batch.finished = true
		batch.err = reason
		delete(s.batches, req)
		s.releaseSlotLocked()
	}
	// Anything still queued is now unfinished; drop the queue since no
	// contiguous advancement can happen after a terminal failure.
	s.queue = nil
	s.finalized = true
	s.markClosedLocked()
	s.signalIdleLocked()
}

// stopAccepting prevents new batches from entering the session while leaving
// already in-flight batches pending so their responses can be drained. It is
// idempotent.
func (s *pipelineSession) stopAccepting() {
	s.lock.Lock()
	s.markClosedLocked()
	s.lock.Unlock()
}

// close ends the session: new sends are rejected, every unfinished batch is
// finalized once with reason, and the underlying transport pipeline is
// closed. It is idempotent.
func (s *pipelineSession) close(reason error) {
	s.lock.Lock()
	if !s.finalized {
		s.failAllLocked(reason)
	}
	s.markClosedLocked()
	s.lock.Unlock()
	s.closeTransport()
}

// closeTransport closes the underlying pipeline at most once. It may be
// called from the decoder to unblock a sender stuck in the transport.
func (s *pipelineSession) closeTransport() {
	s.lock.Lock()
	if s.transportClosed {
		s.lock.Unlock()
		return
	}
	s.transportClosed = true
	pipeline := s.pipeline
	s.lock.Unlock()
	_ = pipeline.Close()
}

// markClosedLocked flips the closed flag and closes the shutdown channel.
// It is called from several terminal paths, so channel closing is guarded by
// the flag. Must be called with the lock held.
func (s *pipelineSession) markClosedLocked() {
	if s.closed {
		return
	}
	s.closed = true
	close(s.shutdownCh)
}

// waitEmpty blocks until every batch has been finalized and no in-flight slot
// is held, until stopCh fires (typically Raft shutdown), or until timeout
// elapses. It is used while draining a closing session so responses already in
// flight are consumed instead of being abandoned.
func (s *pipelineSession) waitEmpty(stopCh <-chan struct{}, timeout time.Duration) bool {
	s.lock.Lock()
	idleCh := s.idleCh
	done := s.idleClosed
	s.lock.Unlock()
	if done {
		return true
	}

	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		timeoutCh = timer.C
	}

	select {
	case <-idleCh:
		return true
	case <-stopCh:
		return false
	case <-timeoutCh:
		return false
	}
}

// signalIdleLocked closes the idle channel once no batch or slot is pending.
// Must be called with the lock held.
func (s *pipelineSession) signalIdleLocked() {
	if s.idleClosed || len(s.batches) != 0 || len(s.inflight) != 0 {
		return
	}
	s.idleClosed = true
	close(s.idleCh)
}

// inflightCount reports how many batches are currently outstanding. It is
// used by tests and diagnostics.
func (s *pipelineSession) inflightCount() int {
	s.lock.Lock()
	defer s.lock.Unlock()
	return len(s.batches)
}

// isClosed reports whether the session stopped accepting new batches.
func (s *pipelineSession) isClosed() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.closed
}

// isFinalized reports whether every tracked batch has been finalized.
func (s *pipelineSession) isFinalized() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.finalized
}

// releaseSlot releases one in-flight semaphore slot; callers must not hold the
// session lock.
func (s *pipelineSession) releaseSlot() {
	<-s.inflight
}

// releaseSlotLocked is releaseSlot for callers that hold the session lock.
// Sends never hold the lock while acquiring a slot, so draining one here
// cannot deadlock.
func (s *pipelineSession) releaseSlotLocked() {
	<-s.inflight
}

// nextPipelineGen allocates the generation id for a new pipeline session on a
// follower replication state. It is only called from the follower's single
// replication goroutine, so it needs no synchronization.
func nextPipelineGen(gen *uint64) uint64 {
	*gen++
	return *gen
}

// replicatedFrontier converts the next log index to send into the highest
// index already known to be replicated. nextIndex is always at least 1, so a
// fresh replication state reports a zero frontier.
func replicatedFrontier(nextIndex uint64) uint64 {
	if nextIndex == 0 {
		return 0
	}
	return nextIndex - 1
}
