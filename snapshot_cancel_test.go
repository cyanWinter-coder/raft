// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

// blockingSnapshotSink blocks Persist in Write until released, so a test can
// cancel a snapshot while it is in flight. It also records which lifecycle
// methods ran so cleanup can be asserted.
type blockingSnapshotSink struct {
	id        string
	store     *blockingSnapshotStore
	startedCh chan struct{}
	releaseCh chan struct{}

	mu       sync.Mutex
	closed   bool
	canceled bool
	buf      bytes.Buffer
}

func newBlockingSnapshotSink() *blockingSnapshotSink {
	return &blockingSnapshotSink{
		id:        "blocking",
		startedCh: make(chan struct{}),
		releaseCh: make(chan struct{}),
	}
}

func (b *blockingSnapshotSink) Write(p []byte) (int, error) {
	closeOnce(b.startedCh)
	<-b.releaseCh
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *blockingSnapshotSink) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	if b.store != nil {
		if b.store.published == nil {
			b.store.published = make(map[string][]byte)
		}
		b.store.published[b.id] = append([]byte(nil), b.buf.Bytes()...)
	}
	return nil
}

func (b *blockingSnapshotSink) Cancel() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.canceled = true
	return nil
}

func (b *blockingSnapshotSink) ID() string { return b.id }

func (b *blockingSnapshotSink) release() {
	select {
	case <-b.releaseCh:
	default:
		close(b.releaseCh)
	}
}

func (b *blockingSnapshotSink) state() (closed, canceled bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed, b.canceled
}

// closeOnce closes ch at most once.
func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// blockingSnapshotStore hands out a single blocking sink and records any
// snapshot that is successfully published so tests can observe publication.
type blockingSnapshotStore struct {
	sink *blockingSnapshotSink

	mu        sync.Mutex
	meta      *SnapshotMeta
	published map[string][]byte
}

func (s *blockingSnapshotStore) Create(version SnapshotVersion, index, term uint64,
	configuration Configuration, configurationIndex uint64, trans Transport) (SnapshotSink, error) {
	s.mu.Lock()
	s.meta = &SnapshotMeta{
		Version:            version,
		ID:                 s.sink.ID(),
		Index:              index,
		Term:               term,
		Configuration:      configuration,
		ConfigurationIndex: configurationIndex,
	}
	s.mu.Unlock()
	return s.sink, nil
}

func (s *blockingSnapshotStore) List() ([]*SnapshotMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.published) == 0 {
		return nil, nil
	}
	return []*SnapshotMeta{s.meta}, nil
}

func (s *blockingSnapshotStore) Open(id string) (*SnapshotMeta, io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.published[id]
	if !ok || s.meta == nil || s.meta.ID != id {
		return nil, nil, fmt.Errorf("snapshot %q not found", id)
	}
	meta := *s.meta
	return &meta, io.NopCloser(bytes.NewReader(data)), nil
}

// blockingFSM blocks inside Persist and otherwise behaves like a minimal FSM.
type blockingFSM struct {
	sink *blockingSnapshotSink
}

func (f *blockingFSM) Apply(*Log) interface{} { return nil }

func (f *blockingFSM) Snapshot() (FSMSnapshot, error) {
	return f, nil
}

func (f *blockingFSM) Restore(io.ReadCloser) error { return nil }

func (f *blockingFSM) Persist(sink SnapshotSink) error {
	if _, err := sink.Write([]byte("data")); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (f *blockingFSM) Release() {}

// TestCancellableSnapshotSink_WriteUnblocksOnCancel verifies that an FSM
// blocked in Persist returns promptly once the snapshot is canceled.
func TestCancellableSnapshotSink_WriteUnblocksOnCancel(t *testing.T) {
	inner := newBlockingSnapshotSink()
	t.Cleanup(inner.release)
	ctx, cancel := context.WithCancel(context.Background())
	future := &userSnapshotFuture{}
	future.initWithContext(ctx)
	sink := newCancellableSnapshotSink(inner, ctx, future.cancelCh(), future)

	writeErr := make(chan error, 1)
	go func() {
		_, err := sink.Write([]byte("data"))
		writeErr <- err
	}()

	select {
	case <-inner.startedCh:
	case <-time.After(time.Second):
		t.Fatal("sink Write never started")
	}

	cancel()

	select {
	case err := <-writeErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Write was not unblocked by cancellation")
	}

	// The underlying write is still draining. Release it so Cancel can wait
	// for the in-flight write to finish before discarding the sink.
	inner.release()

	if err := sink.Cancel(); err != nil {
		t.Fatalf("Cancel failed: %v", err)
	}
	if closed, canceled := inner.state(); closed || !canceled {
		t.Fatalf("inner sink state closed=%v canceled=%v, want closed=false canceled=true", closed, canceled)
	}

	// A subsequent Close must not reach the inner sink after Cancel; it
	// reports the cancellation outcome rather than publishing.
	if err := sink.Close(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close after Cancel should report context.Canceled, got %v", err)
	}
	if closed, _ := inner.state(); closed {
		t.Fatal("inner sink was closed after it had been canceled")
	}
}

// TestCancellableSnapshotSink_NoCancelChannelIsPassThrough verifies that
// automatic and non-cancelable snapshots keep the original sink semantics.
func TestCancellableSnapshotSink_NoCancelChannelIsPassThrough(t *testing.T) {
	inner := newBlockingSnapshotSink()
	t.Cleanup(inner.release)
	sink := newCancellableSnapshotSink(inner, nil, nil, nil)

	inner.release()
	if n, err := sink.Write([]byte("data")); err != nil || n != 4 {
		t.Fatalf("Write returned n=%d err=%v", n, err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if closed, canceled := inner.state(); !closed || canceled {
		t.Fatalf("inner sink state closed=%v canceled=%v, want closed=true canceled=false", closed, canceled)
	}
}

// TestUserSnapshotFuture_CommitAndCancelAreMutuallyExclusive verifies the
// arbitration state machine never allows a snapshot to be both committed and
// canceled, regardless of the order the two operations arrive in.
func TestUserSnapshotFuture_CommitAndCancelAreMutuallyExclusive(t *testing.T) {
	t.Run("cancel then commit loses", func(t *testing.T) {
		f := &userSnapshotFuture{}
		ctx, cancel := context.WithCancel(context.Background())
		f.initWithContext(ctx)

		cancel()
		if !f.markCanceled() {
			t.Fatal("first markCanceled should win")
		}
		if f.tryCommit() {
			t.Fatal("tryCommit must lose after cancellation")
		}
		if f.tryCommit() {
			t.Fatal("tryCommit stays losing on repeated calls")
		}
		if !f.canceled() {
			t.Fatal("future must report canceled")
		}
	})

	t.Run("commit then cancel loses", func(t *testing.T) {
		f := &userSnapshotFuture{}
		ctx, cancel := context.WithCancel(context.Background())
		f.initWithContext(ctx)

		if !f.tryCommit() {
			t.Fatal("first tryCommit should win from pending")
		}
		if f.tryCommit() != true {
			t.Fatal("tryCommit stays winning on repeated calls")
		}
		if f.markCanceled() {
			t.Fatal("markCanceled must lose after commit")
		}
		if f.canceled() {
			t.Fatal("a committed future must never report canceled")
		}
		cancel()
	})

	t.Run("commit wins with pending context", func(t *testing.T) {
		f := &userSnapshotFuture{}
		f.initWithContext(context.Background())
		if !f.tryCommit() {
			t.Fatal("tryCommit should win while the context is pending")
		}
	})
}

// TestUserSnapshotFuture_ErrorHonorsCommittedState ensures a snapshot that
// committed before the context was canceled still resolves to success.
func TestUserSnapshotFuture_ErrorHonorsCommittedState(t *testing.T) {
	f := &userSnapshotFuture{}
	ctx, cancel := context.WithCancel(context.Background())
	f.initWithContext(ctx)

	if !f.tryCommit() {
		t.Fatal("tryCommit should win")
	}
	cancel()
	// The response would be delivered by finishUserSnapshot in production.
	f.respond(nil)

	if err := f.Error(); err != nil {
		t.Fatalf("committed snapshot must resolve to success after a late cancel, got %v", err)
	}
}

// newCancellableTestRaft builds a single-node Raft on top of a blocking
// snapshot store so a snapshot can be stalled mid-Persist.
func newCancellableTestRaft(t *testing.T) (*Raft, *blockingSnapshotStore, *blockingSnapshotSink) {
	t.Helper()
	conf := inmemConfig(t)
	conf.LocalID = ServerID("test-node")

	sink := newBlockingSnapshotSink()
	store := &blockingSnapshotStore{sink: sink, published: make(map[string][]byte)}
	sink.store = store
	fsm := &blockingFSM{sink: sink}

	logs := NewInmemStore()
	stable := NewInmemStore()
	_, trans := NewInmemTransportWithTimeout(NewInmemAddr(), time.Second)

	if err := BootstrapCluster(conf, logs, stable, store, trans, Configuration{
		Servers: []Server{{
			Suffrage: Voter,
			ID:       conf.LocalID,
			Address:  trans.LocalAddr(),
		}},
	}); err != nil {
		t.Fatalf("bootstrap failed: %v", err)
	}

	r, err := NewRaft(conf, fsm, logs, stable, store, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	t.Cleanup(func() {
		sink.release()
		_ = r.Shutdown().Error()
	})
	return r, store, sink
}

// waitForSingleNodeLeader blocks until the single node becomes leader.
func waitForSingleNodeLeader(t *testing.T, r *Raft) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if r.Leader() != "" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("node never became leader")
}

// TestRaft_SnapshotWithContext_CanceledDuringPersist exercises the main
// cancellation path: the future reports the context error promptly, the
// unfinished snapshot is never published, and store resources are discarded.
func TestRaft_SnapshotWithContext_CanceledDuringPersist(t *testing.T) {
	r, store, sink := newCancellableTestRaft(t)
	waitForSingleNodeLeader(t, r)

	if err := r.Apply([]byte("value"), time.Second).Error(); err != nil {
		t.Fatalf("apply failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	future := r.SnapshotWithContext(ctx)

	select {
	case <-sink.startedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot Persist never started")
	}

	// Give up while the FSM is blocked writing the snapshot.
	cancel()

	done := make(chan error, 1)
	go func() { done <- future.Error() }()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("future did not return promptly after cancellation")
	}

	// The error is sticky: repeated calls report the same outcome.
	if err := future.Error(); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected sticky context.Canceled, got %v", err)
	}

	// Let the blocked Persist unwind; cleanup must discard the sink and must
	// never publish it as the latest stable snapshot.
	sink.release()

	// Wait for the snapshot goroutine to finish its cleanup.
	waitUntil(t, func() bool {
		closed, canceled := sink.state()
		return closed || canceled
	}, 5*time.Second)
	if closed, canceled := sink.state(); closed {
		t.Fatalf("canceled snapshot was closed/published (closed=%v canceled=%v)", closed, canceled)
	} else if !canceled {
		t.Fatal("sink resources were not canceled after the snapshot was abandoned")
	}

	if snaps, err := store.List(); err != nil || len(snaps) != 0 {
		t.Fatalf("canceled snapshot must not be published, got %v (err=%v)", snaps, err)
	}

	lastSnapIndex, _ := r.getLastSnapshot()
	if lastSnapIndex != 0 {
		t.Fatalf("last stable snapshot must not advance on cancellation, got index %d", lastSnapIndex)
	}
}

// TestRaft_SnapshotWithContext_AlreadyCanceled verifies a context canceled
// before the request is even dispatched fails without creating store
// resources.
func TestRaft_SnapshotWithContext_AlreadyCanceled(t *testing.T) {
	r, _, sink := newCancellableTestRaft(t)
	waitForSingleNodeLeader(t, r)

	if err := r.Apply([]byte("value"), time.Second).Error(); err != nil {
		t.Fatalf("apply failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	future := r.SnapshotWithContext(ctx)
	if err := future.Error(); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// The FSM should never have been asked to persist anything.
	select {
	case <-sink.startedCh:
		t.Fatal("snapshot Persist ran despite an already-canceled context")
	case <-time.After(100 * time.Millisecond):
	}
}

// TestRaft_SnapshotWithContext_CancelBeforeFinalize deterministically forces
// cancellation to win by keeping the sink blocked until after the context is
// canceled and the cancellation callback has run, only then releasing it. The
// snapshot must be discarded, never published, and never compacted.
func TestRaft_SnapshotWithContext_CancelBeforeFinalize(t *testing.T) {
	r, store, sink := newCancellableTestRaft(t)
	waitForSingleNodeLeader(t, r)

	if err := r.Apply([]byte("value"), time.Second).Error(); err != nil {
		t.Fatalf("apply failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	future := r.SnapshotWithContext(ctx)

	select {
	case <-sink.startedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot Persist never started")
	}

	// Cancel while the sink write is blocked, then give the cancellation
	// callback time to flip the future before the write is released.
	cancel()
	if err := future.Error(); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	// Allow the AfterFunc callback to run and settle the canceled state.
	userFuture := future.(*userSnapshotFuture)
	waitUntil(t, userFuture.canceled, time.Second)

	// Now let the blocked write finish. The sink must still be discarded,
	// not published, because cancellation was decided before finalization.
	sink.release()
	waitUntil(t, func() bool {
		closed, canceled := sink.state()
		return closed || canceled
	}, 5*time.Second)

	if closed, canceled := sink.state(); closed || !canceled {
		t.Fatalf("sink state closed=%v canceled=%v, want closed=false canceled=true", closed, canceled)
	}
	if snaps, _ := store.List(); len(snaps) != 0 {
		t.Fatalf("canceled snapshot must not be published, got %d", len(snaps))
	}
	if idx, _ := r.getLastSnapshot(); idx != 0 {
		t.Fatalf("canceled snapshot must not advance stable index, got %d", idx)
	}
}

// TestRaft_SnapshotWithContext_CompleteBeforeCancel deterministically forces
// success to win: let the snapshot fully finalize, only then cancel the
// context. The completed snapshot must remain published and report success.
func TestRaft_SnapshotWithContext_CompleteBeforeCancel(t *testing.T) {
	r, _, sink := newCancellableTestRaft(t)
	waitForSingleNodeLeader(t, r)

	if err := r.Apply([]byte("value"), time.Second).Error(); err != nil {
		t.Fatalf("apply failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	future := r.SnapshotWithContext(ctx)

	select {
	case <-sink.startedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot Persist never started")
	}

	// Let the snapshot finish and settle before canceling.
	sink.release()
	if err := future.Error(); err != nil {
		t.Fatalf("snapshot should succeed: %v", err)
	}
	cancel()

	if idx, _ := r.getLastSnapshot(); idx == 0 {
		t.Fatal("completed snapshot must advance the stable index")
	}
	// The late cancellation must not change the already-reported success.
	if err := future.Error(); err != nil {
		t.Fatalf("late cancellation altered snapshot outcome: %v", err)
	}
	meta, reader, err := future.Open()
	if err != nil {
		t.Fatalf("completed snapshot could not be opened: %v", err)
	}
	if meta == nil || meta.Index == 0 {
		t.Fatalf("bad metadata: %+v", meta)
	}
	_ = reader.Close()
}

// waitUntil polls condition until it is true or the deadline passes.
func waitUntil(t *testing.T, condition func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

// TestRaft_SnapshotWithContext_SuccessBehavesLikeSnapshot verifies that with
// a context that is never canceled, the cancelable entry point matches the
// legacy Snapshot() behavior including stable-snapshot advancement.
func TestRaft_SnapshotWithContext_SuccessBehavesLikeSnapshot(t *testing.T) {
	r, _, sink := newCancellableTestRaft(t)
	waitForSingleNodeLeader(t, r)

	if err := r.Apply([]byte("value"), time.Second).Error(); err != nil {
		t.Fatalf("apply failed: %v", err)
	}

	future := r.SnapshotWithContext(context.Background())
	sink.release()

	if err := future.Error(); err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}
	meta, reader, err := future.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if meta == nil || meta.Index == 0 {
		t.Fatalf("bad snapshot metadata: %+v", meta)
	}
	_ = reader.Close()

	if idx, _ := r.getLastSnapshot(); idx == 0 {
		t.Fatal("successful snapshot did not advance the stable snapshot index")
	}
}
