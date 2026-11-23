// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeAppendFuture is a test AppendFuture whose completion is driven by the
// fakePipeline. It records how many times it was responded to so tests can
// assert exactly-once completion.
type fakeAppendFuture struct {
	deferError
	start time.Time
	args  *AppendEntriesRequest
	resp  *AppendEntriesResponse

	lock        sync.Mutex
	responded   int
	respondedCh chan struct{}
}

func (f *fakeAppendFuture) Start() time.Time               { return f.start }
func (f *fakeAppendFuture) Request() *AppendEntriesRequest { return f.args }
func (f *fakeAppendFuture) Response() *AppendEntriesResponse {
	return f.resp
}

// respondOnce completes the future at most once, mirroring the exactly-once
// contract of the real transports.
func (f *fakeAppendFuture) respondOnce(err error) {
	f.lock.Lock()
	if f.responded > 0 {
		f.lock.Unlock()
		return
	}
	f.responded++
	respCh := f.respondedCh
	f.lock.Unlock()

	f.respond(err)
	close(respCh)
}

func (f *fakeAppendFuture) responseCount() int {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.responded
}

// fakePipeline is a deterministic in-process AppendPipeline used to exercise
// pipelineSession without any network timing. Responses can be delivered in
// arbitrary order.
type fakePipeline struct {
	lock    sync.Mutex
	closed  bool
	sendErr error // if non-nil, AppendEntries fails with it
	sent    []*fakeAppendFuture
	doneCh  chan AppendFuture
}

func newFakePipeline() *fakePipeline {
	return &fakePipeline{
		// Large buffer so deterministic tests never block on delivery; the
		// session's own semaphore is what bounds real concurrency.
		doneCh: make(chan AppendFuture, 1024),
	}
}

func (f *fakePipeline) AppendEntries(args *AppendEntriesRequest, resp *AppendEntriesResponse) (AppendFuture, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.closed {
		return nil, ErrPipelineShutdown
	}
	if f.sendErr != nil {
		return nil, f.sendErr
	}
	fut := &fakeAppendFuture{
		start:       time.Now(),
		args:        args,
		resp:        resp,
		respondedCh: make(chan struct{}),
	}
	fut.init()
	f.sent = append(f.sent, fut)
	return fut, nil
}

func (f *fakePipeline) Consumer() <-chan AppendFuture { return f.doneCh }

func (f *fakePipeline) Close() error {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	return nil
}

func (f *fakePipeline) isClosed() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.closed
}

// deliver completes the nth sent future with resp and err and hands it to the
// consumer channel, simulating a response arriving (possibly out of order).
func (f *fakePipeline) deliver(n int, resp *AppendEntriesResponse, err error) AppendFuture {
	f.lock.Lock()
	fut := f.sent[n]
	f.lock.Unlock()

	if resp != nil {
		*fut.resp = *resp
	}
	fut.respondOnce(err)
	f.doneCh <- fut
	return fut
}

func appendReq(index, term uint64) *AppendEntriesRequest {
	return &AppendEntriesRequest{
		Term: term,
		Entries: []*Log{
			{Index: index, Term: term, Type: LogCommand},
		},
	}
}

func successResp(term uint64) *AppendEntriesResponse {
	return &AppendEntriesResponse{Term: term, Success: true}
}

func drainResult(t *testing.T, session *pipelineSession, fp *fakePipeline) pipelineBatchResult {
	t.Helper()
	select {
	case fut := <-fp.doneCh:
		return session.handle(fut)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for response")
		return pipelineBatchResult{}
	}
}

// TestPipelineSession_ContiguousAdvancement verifies requirement 2: successes
// arriving out of order must not advance the replicated position over a gap.
func TestPipelineSession_ContiguousAdvancement(t *testing.T) {
	fp := newFakePipeline()
	defer func() { _ = fp.Close() }()
	session := newPipelineSession(7, fp)

	reqs := []*AppendEntriesRequest{
		appendReq(10, 2),
		appendReq(11, 2),
		appendReq(12, 2),
	}
	for _, req := range reqs {
		_, err := session.send(req)
		require.NoError(t, err)
	}
	require.Equal(t, 3, session.inflightCount())

	// Deliver the last batch's success first.
	fp.deliver(2, successResp(2), nil)
	res := drainResult(t, session, fp)
	require.Equal(t, pipelineOutcomeGap, res.outcome)
	require.Equal(t, uint64(0), session.frontier())

	// Middle success still cannot move anything because batch 0 is a gap.
	fp.deliver(1, successResp(2), nil)
	res = drainResult(t, session, fp)
	require.Equal(t, pipelineOutcomeGap, res.outcome)
	require.Equal(t, uint64(0), session.frontier())
	require.Equal(t, 1, session.inflightCount())

	// Completing the head releases the whole contiguous run in order.
	fp.deliver(0, successResp(2), nil)
	res = drainResult(t, session, fp)
	require.Equal(t, pipelineOutcomeAdvanced, res.outcome)
	require.Equal(t, uint64(12), res.matchIndex)
	require.Equal(t, uint64(12), session.frontier())
	require.Equal(t, 0, session.inflightCount())
}

// TestPipelineSession_NoEntryBatchDoesNotStall verifies that a successful
// batch carrying no entries (an AppendEntries used to propagate the commit
// index) neither advances the frontier nor blocks the batches behind it.
func TestPipelineSession_NoEntryBatchDoesNotStall(t *testing.T) {
	fp := newFakePipeline()
	defer func() { _ = fp.Close() }()
	session := newPipelineSession(1, fp)

	// First batch is a no-op, the second carries index 5.
	_, err := session.send(&AppendEntriesRequest{Term: 2})
	require.NoError(t, err)
	_, err = session.send(appendReq(5, 2))
	require.NoError(t, err)

	fp.deliver(1, successResp(2), nil)
	res := drainResult(t, session, fp)
	require.Equal(t, pipelineOutcomeGap, res.outcome)

	fp.deliver(0, successResp(2), nil)
	res = drainResult(t, session, fp)
	require.Equal(t, pipelineOutcomeAdvanced, res.outcome)
	require.Equal(t, uint64(5), res.matchIndex)
	require.Equal(t, 0, session.inflightCount())
}

// TestPipelineSession_RejectedBatchFailsAll verifies requirement 3: a rejected
// batch ends every unfinished batch exactly once and no later success can push
// the position over the failure gap.
func TestPipelineSession_RejectedBatchFailsAll(t *testing.T) {
	fp := newFakePipeline()
	defer func() { _ = fp.Close() }()
	session := newPipelineSession(1, fp)

	for _, idx := range []uint64{20, 21, 22, 23} {
		_, err := session.send(appendReq(idx, 2))
		require.NoError(t, err)
	}

	fp.deliver(0, successResp(2), nil)
	res := drainResult(t, session, fp) // 20 succeeds
	require.Equal(t, pipelineOutcomeAdvanced, res.outcome)
	require.Equal(t, uint64(20), res.matchIndex)

	fp.deliver(1, &AppendEntriesResponse{Term: 2, Success: false, LastLog: 19}, nil)
	res = drainResult(t, session, fp) // 21 rejected
	require.Equal(t, pipelineOutcomeRejected, res.outcome)
	require.Equal(t, uint64(19), res.lastLog)

	// Session is closed and every remaining batch was finalized once.
	require.True(t, session.isClosed())
	require.True(t, session.isFinalized())
	require.Equal(t, 0, session.inflightCount())
	require.Equal(t, 0, len(session.inflight))

	// A late success for batch 22 must not change anything, and re-handling it
	// never reports success twice.
	late := fp.deliver(2, successResp(2), nil)
	res = session.handle(late)
	require.Equal(t, pipelineOutcomeStale, res.outcome)
	require.Equal(t, uint64(20), session.frontier())
	res = session.handle(late)
	require.Equal(t, pipelineOutcomeStale, res.outcome)
	require.Equal(t, uint64(20), session.frontier())

	// New sends are refused rather than blocking.
	_, err := session.send(appendReq(24, 2))
	require.ErrorIs(t, err, ErrPipelineShutdown)
}

// TestPipelineSession_RPCErrorFailsAll verifies requirement 3 for transport
// errors.
func TestPipelineSession_RPCErrorFailsAll(t *testing.T) {
	fp := newFakePipeline()
	defer func() { _ = fp.Close() }()
	session := newPipelineSession(1, fp)

	for _, idx := range []uint64{30, 31, 32} {
		_, err := session.send(appendReq(idx, 2))
		require.NoError(t, err)
	}

	rpcErr := errors.New("connection reset")
	fp.deliver(0, nil, rpcErr)
	res := drainResult(t, session, fp)
	require.Equal(t, pipelineOutcomeRPCError, res.outcome)
	require.Equal(t, rpcErr, res.err)
	require.True(t, session.isClosed())
	require.True(t, session.isFinalized())
	require.Equal(t, 0, session.inflightCount())
	require.Equal(t, 0, len(session.inflight))

	// Subsequent responses are stale and cannot resurrect the session.
	fp.deliver(1, successResp(2), nil)
	res = drainResult(t, session, fp)
	require.Equal(t, pipelineOutcomeStale, res.outcome)
	require.Equal(t, uint64(0), session.frontier())
}

// TestPipelineSession_StaleTermFailsAll verifies requirement 3 for term
// changes.
func TestPipelineSession_StaleTermFailsAll(t *testing.T) {
	fp := newFakePipeline()
	defer func() { _ = fp.Close() }()
	session := newPipelineSession(1, fp)

	_, err := session.send(appendReq(40, 2))
	require.NoError(t, err)
	_, err = session.send(appendReq(41, 2))
	require.NoError(t, err)

	fp.deliver(0, &AppendEntriesResponse{Term: 3, Success: false}, nil)
	res := drainResult(t, session, fp)
	require.Equal(t, pipelineOutcomeStaleTerm, res.outcome)
	require.True(t, session.isClosed())
	require.True(t, session.isFinalized())
	require.Equal(t, 0, session.inflightCount())
	require.Equal(t, 0, len(session.inflight))
}

// TestPipelineSession_GenerationIsolation verifies requirement 1: a response
// from an older generation session can only complete its own waiter and must
// not be consumed as state by the newer session.
func TestPipelineSession_GenerationIsolation(t *testing.T) {
	oldFP := newFakePipeline()
	newFP := newFakePipeline()
	defer func() { _ = oldFP.Close() }()
	defer func() { _ = newFP.Close() }()

	var gen uint64
	g1 := nextPipelineGen(&gen)
	g2 := nextPipelineGen(&gen)
	require.Equal(t, uint64(1), g1)
	require.Equal(t, uint64(2), g2)

	oldSession := newPipelineSessionAt(g1, oldFP, 49)
	newSession := newPipelineSessionAt(g2, newFP, 49)

	oldBatch, err := oldSession.send(appendReq(50, 2))
	require.NoError(t, err)
	require.Equal(t, g1, oldBatch.gen)

	newBatch, err := newSession.send(appendReq(60, 2))
	require.NoError(t, err)
	require.Equal(t, g2, newBatch.gen)

	// The old connection's late response reaching the new session must be
	// ignored entirely: no frontier movement and no batch bookkeeping change.
	oldFuture := oldFP.deliver(0, successResp(2), nil)
	res := newSession.handle(oldFuture)
	require.Equal(t, pipelineOutcomeStale, res.outcome)
	require.Equal(t, uint64(49), newSession.frontier())
	require.Equal(t, 1, newSession.inflightCount())

	// The old session can still process its own response and finalize its
	// own waiter.
	res = oldSession.handle(oldFuture)
	require.Equal(t, pipelineOutcomeAdvanced, res.outcome)
	require.Equal(t, uint64(50), res.matchIndex)
	require.Equal(t, 0, oldSession.inflightCount())
}

// TestPipelineSession_CloseFailsAllOnce verifies requirements 3 and 4: close
// stops new sends, finalizes all pending batches once, and is idempotent.
func TestPipelineSession_CloseFailsAllOnce(t *testing.T) {
	fp := newFakePipeline()
	session := newPipelineSession(1, fp)

	batches := make([]*pipelineBatch, 3)
	for i, idx := range []uint64{70, 71, 72} {
		b, err := session.send(appendReq(idx, 2))
		require.NoError(t, err)
		batches[i] = b
	}

	closeErr := errors.New("session closed")
	session.close(closeErr)
	require.True(t, fp.isClosed())
	require.True(t, session.isClosed())
	require.True(t, session.isFinalized())
	require.Equal(t, 0, session.inflightCount())
	require.Equal(t, 0, len(session.inflight))

	session.lock.Lock()
	for _, b := range batches {
		require.True(t, b.finished)
		require.False(t, b.succeeded)
		require.ErrorIs(t, b.err, closeErr)
	}
	session.lock.Unlock()

	// Idempotent: second close does not panic and does not re-finalize.
	require.NotPanics(t, func() { session.close(closeErr) })

	// New sends rejected.
	_, err := session.send(appendReq(73, 2))
	require.ErrorIs(t, err, ErrPipelineShutdown)
}

// TestPipelineSession_ShutdownThenDrain verifies requirement 4: after shutdown
// no new batches are accepted, but responses already started still drain
// through handle and advance the frontier until the in-flight set is empty.
func TestPipelineSession_ShutdownThenDrain(t *testing.T) {
	fp := newFakePipeline()
	defer func() { _ = fp.Close() }()
	session := newPipelineSession(1, fp)

	for _, idx := range []uint64{80, 81, 82} {
		_, err := session.send(appendReq(idx, 2))
		require.NoError(t, err)
	}

	session.stopAccepting()
	require.True(t, session.isClosed())
	require.False(t, session.isFinalized())
	_, err := session.send(appendReq(83, 2))
	require.ErrorIs(t, err, ErrPipelineShutdown)

	// In-flight batches still exist and their responses drain normally.
	require.Equal(t, 3, session.inflightCount())
	for i := 0; i < 3; i++ {
		fp.deliver(i, successResp(2), nil)
	}
	for _, want := range []uint64{80, 81, 82} {
		res := drainResult(t, session, fp)
		require.Equal(t, pipelineOutcomeAdvanced, res.outcome)
		require.Equal(t, want, res.matchIndex)
	}

	// waitEmpty now returns immediately.
	stop := make(chan struct{})
	require.True(t, session.waitEmpty(stop, time.Second))

	// Final close finalizes nothing extra and is harmless.
	session.close(ErrPipelineShutdown)
	require.True(t, session.isFinalized())
}

// TestPipelineSession_WaitEmptyInterrupted verifies waitEmpty honors the stop
// channel and a timeout so raft shutdown cannot be blocked by a stuck
// transport.
func TestPipelineSession_WaitEmptyInterrupted(t *testing.T) {
	fp := newFakePipeline()
	defer func() { _ = fp.Close() }()
	session := newPipelineSession(1, fp)

	_, err := session.send(appendReq(90, 2))
	require.NoError(t, err)

	// Timeout path.
	require.False(t, session.waitEmpty(make(chan struct{}), 10*time.Millisecond))

	// Stop path.
	stop := make(chan struct{})
	close(stop)
	require.False(t, session.waitEmpty(stop, time.Minute))
}

// TestPipelineSession_BoundedConcurrency verifies requirement 4: send blocks
// once the in-flight bound is reached and unblocks after finalization or
// shutdown.
func TestPipelineSession_BoundedConcurrency(t *testing.T) {
	fp := newFakePipeline()
	defer func() { _ = fp.Close() }()
	session := newPipelineSession(1, fp)

	for i := 0; i < maxPipelineInflight; i++ {
		_, err := session.send(appendReq(uint64(100+i), 2))
		require.NoError(t, err)
	}
	require.Equal(t, maxPipelineInflight, len(session.inflight))

	// The next send blocks until a slot frees.
	sent := make(chan error, 1)
	go func() {
		_, err := session.send(appendReq(999, 2))
		sent <- err
	}()
	select {
	case <-sent:
		t.Fatal("send should have blocked at the in-flight bound")
	case <-time.After(20 * time.Millisecond):
	}

	// Complete one response; the blocked send can proceed.
	fp.deliver(0, successResp(2), nil)
	_ = drainResult(t, session, fp)
	select {
	case err := <-sent:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("blocked send did not unblock after a slot freed")
	}

	// Filling again and shutting down must release any blocked sender with
	// ErrPipelineShutdown.
	for len(session.inflight) < maxPipelineInflight {
		_, err := session.send(appendReq(uint64(1000+len(session.inflight)), 2))
		require.NoError(t, err)
	}
	blocked := make(chan error, 1)
	go func() {
		_, err := session.send(appendReq(5000, 2))
		blocked <- err
	}()
	time.Sleep(10 * time.Millisecond)
	session.stopAccepting()
	select {
	case err := <-blocked:
		require.ErrorIs(t, err, ErrPipelineShutdown)
	case <-time.After(time.Second):
		t.Fatal("blocked send did not unblock on shutdown")
	}
}

// TestPipelineSession_SendErrorFailsSession verifies a transport-level send
// failure ends the session, releases every slot, and closes the transport
// pipeline so a stuck sender is released.
func TestPipelineSession_SendErrorFailsSession(t *testing.T) {
	fp := newFakePipeline()
	session := newPipelineSession(1, fp)

	_, err := session.send(appendReq(110, 2))
	require.NoError(t, err)

	sendErr := errors.New("pipe broken")
	fp.sendErr = sendErr
	_, err = session.send(appendReq(111, 2))
	require.ErrorIs(t, err, sendErr)
	require.True(t, session.isClosed())
	require.True(t, session.isFinalized())
	require.True(t, fp.isClosed())
	require.Equal(t, 0, len(session.inflight))
	require.Equal(t, 0, session.inflightCount())
}
