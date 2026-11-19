// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	metrics "github.com/hashicorp/go-metrics"
)

// SnapshotMeta is for metadata of a snapshot.
type SnapshotMeta struct {
	// Version is the version number of the snapshot metadata. This does not cover
	// the application's data in the snapshot, that should be versioned
	// separately.
	Version SnapshotVersion

	// ID is opaque to the store, and is used for opening.
	ID string

	// Index and Term store when the snapshot was taken.
	Index uint64
	Term  uint64

	// Peers is deprecated and used to support version 0 snapshots, but will
	// be populated in version 1 snapshots as well to help with upgrades.
	Peers []byte

	// Configuration and ConfigurationIndex are present in version 1
	// snapshots and later.
	Configuration      Configuration
	ConfigurationIndex uint64

	// Size is the size of the snapshot in bytes.
	Size int64
}

// SnapshotStore interface is used to allow for flexible implementations
// of snapshot storage and retrieval. For example, a client could implement
// a shared state store such as S3, allowing new nodes to restore snapshots
// without streaming from the leader.
type SnapshotStore interface {
	// Create is used to begin a snapshot at a given index and term, and with
	// the given committed configuration. The version parameter controls
	// which snapshot version to create.
	Create(version SnapshotVersion, index, term uint64, configuration Configuration,
		configurationIndex uint64, trans Transport) (SnapshotSink, error)

	// List is used to list the available snapshots in the store.
	// It should return then in descending order, with the highest index first.
	List() ([]*SnapshotMeta, error)

	// Open takes a snapshot ID and provides a ReadCloser. Once close is
	// called it is assumed the snapshot is no longer needed.
	Open(id string) (*SnapshotMeta, io.ReadCloser, error)
}

// SnapshotSink is returned by StartSnapshot. The FSM will Write state
// to the sink and call Close on completion. On error, Cancel will be invoked.
type SnapshotSink interface {
	io.WriteCloser
	ID() string
	Cancel() error
}

// cancellableSnapshotSink wraps a SnapshotSink so a cancelable snapshot
// can interrupt an FSM blocked in Persist and so that Close and Cancel are
// each applied to the underlying sink at most once, no matter how the FSM's
// Persist and Raft's own cleanup interleave. A nil future means the snapshot
// can not be canceled; the wrapper then behaves as a pass-through.
type cancellableSnapshotSink struct {
	inner    SnapshotSink
	ctx      context.Context
	cancelCh <-chan struct{}
	// future, when set, is the authoritative arbitration point. Close
	// atomically claims the committed state on its mutex, so a snapshot can
	// never be published once the context has won the race.
	future *userSnapshotFuture

	mu sync.Mutex
	// finalized records the terminal action already applied to the
	// underlying sink ("close", "cancel", or "") so it never happens twice.
	finalized string
	// closeResult is the outcome of the first Close, returned again to any
	// later caller. In particular a Close that loses the cancellation race
	// keeps returning context.Canceled instead of being masked as success,
	// which matters because both the FSM's Persist and Raft may call Close.
	closeResult error
	// inFlight tracks writes handed to the underlying sink so Close and
	// Cancel never touch the sink while a write is still running, which
	// would race the sink's internal buffers and file handles.
	inFlight sync.WaitGroup
}

// newCancellableSnapshotSink wraps sink. ctx is the authoritative
// cancellation signal and cancelCh a second trigger so a blocked write is
// woken even if the context.AfterFunc callback is scheduled slightly late.
// future arbitrates publication and is nil for non-cancelable snapshots.
func newCancellableSnapshotSink(sink SnapshotSink, ctx context.Context, cancelCh <-chan struct{}, future *userSnapshotFuture) *cancellableSnapshotSink {
	return &cancellableSnapshotSink{inner: sink, ctx: ctx, cancelCh: cancelCh, future: future}
}

// Write forwards p to the underlying sink, racing the write against
// cancellation. When the context is canceled while a write to slow storage is
// in flight, Write returns the context error promptly; the underlying write
// is allowed to finish in the background and is drained before Close or
// Cancel touches the sink, so the sink is never used concurrently.
func (s *cancellableSnapshotSink) Write(p []byte) (int, error) {
	if s.ctx == nil {
		return s.inner.Write(p)
	}

	// Take ownership of the bytes: the caller may reuse the buffer after
	// Write returns on cancellation, while the background write still needs
	// them.
	buf := make([]byte, len(p))
	copy(buf, p)

	type writeResult struct {
		n   int
		err error
	}
	resultCh := make(chan writeResult, 1)
	s.inFlight.Add(1)
	go func() {
		defer s.inFlight.Done()
		n, err := s.inner.Write(buf)
		resultCh <- writeResult{n, err}
	}()

	select {
	case res := <-resultCh:
		return res.n, res.err
	case <-s.ctx.Done():
		return 0, s.ctx.Err()
	case <-s.cancelCh:
		return 0, context.Canceled
	}
}

// ID returns the ID of the underlying sink.
func (s *cancellableSnapshotSink) ID() string {
	return s.inner.ID()
}

// Close finalizes the underlying sink, publishing the snapshot, but only if
// cancellation has not already won. The publication decision is made by
// tryCommit under the future's mutex, where it is atomic with the state
// transition to either committed or canceled; there is no check-then-act
// window. It fails with context.Canceled when the context won, leaving the
// caller to invoke Cancel. A non-cancelable snapshot (nil future) always
// commits. Repeated calls return the original outcome rather than masking a
// lost race as success.
func (s *cancellableSnapshotSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finalized == "close" {
		return s.closeResult
	}
	if s.finalized == "cancel" {
		// Already discarded; report cancellation to any later Close.
		return context.Canceled
	}
	if s.future != nil && !s.future.tryCommit() {
		s.closeResult = context.Canceled
		return s.closeResult
	}
	s.finalized = "close"
	s.inFlight.Wait()
	s.closeResult = s.inner.Close()
	return s.closeResult
}

// Cancel discards the underlying sink exactly once, after any in-flight
// writes have drained. It first claims the canceled outcome on the future,
// matching what context cancellation does, so a later Close can never
// publish the snapshot (tryCommit will then fail). It is safe to call after
// the FSM's Persist has already called Close or Cancel; when Close already
// won the committed outcome, Cancel is a no-op.
func (s *cancellableSnapshotSink) Cancel() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finalized != "" {
		return nil
	}
	// Claim the canceled outcome. If success was already committed this
	// returns false, but in that case Close would have set finalized first,
	// so reaching here means cancellation legitimately wins.
	if s.future != nil {
		s.future.markCanceled()
	}
	s.finalized = "cancel"
	s.inFlight.Wait()
	return s.inner.Cancel()
}

// runSnapshots is a long running goroutine used to manage taking
// new snapshots of the FSM. It runs in parallel to the FSM and
// main goroutines, so that snapshots do not block normal operation.
func (r *Raft) runSnapshots() {
	for {
		select {
		case <-randomTimeout(r.config().SnapshotInterval):
			// Check if we should snapshot
			if !r.shouldSnapshot() {
				continue
			}

			// Trigger a snapshot. Automatic snapshots are not cancelable,
			// so their behavior is unchanged.
			if _, err := r.takeSnapshot(context.Background(), nil); err != nil && !errors.Is(err, ErrNothingNewToSnapshot) {
				r.logger.Error("failed to take snapshot", "error", err)
			}

		case future := <-r.userSnapshotCh:
			// User-triggered, run immediately
			id, err := r.takeSnapshot(future.ctx, future)
			r.finishUserSnapshot(future, id, err)

		case <-r.shutdownCh:
			return
		}
	}
}

// finishUserSnapshot resolves a user-triggered snapshot future. It applies
// the same arbitration as the snapshot work itself: a canceled snapshot is
// reported with the context error even if the work unwound with a different
// error, and a snapshot that completed before cancellation is reported as a
// success. The future's own state machine makes sure exactly one outcome is
// ever reported, regardless of how completion and cancellation interleave.
func (r *Raft) finishUserSnapshot(future *userSnapshotFuture, id string, takeErr error) {
	switch {
	case future.canceled():
		// Cancellation won. takeSnapshot is responsible for having discarded
		// any resources the snapshot store created.
		future.respond(future.ctx.Err())
	case takeErr != nil:
		if !errors.Is(takeErr, ErrNothingNewToSnapshot) {
			r.logger.Error("failed to take snapshot", "error", takeErr)
		}
		future.respond(takeErr)
	default:
		future.opener = func() (*SnapshotMeta, io.ReadCloser, error) {
			return r.snapshots.Open(id)
		}
		future.respond(nil)
	}
}

// shouldSnapshot checks if we meet the conditions to take
// a new snapshot.
func (r *Raft) shouldSnapshot() bool {
	// Check the last snapshot index
	lastSnap, _ := r.getLastSnapshot()

	// Check the last log index
	lastIdx, err := r.logs.LastIndex()
	if err != nil {
		r.logger.Error("failed to get last log index", "error", err)
		return false
	}

	// Compare the delta to the threshold
	delta := lastIdx - lastSnap
	return delta >= r.config().SnapshotThreshold
}

// takeSnapshot is used to take a new snapshot. This must only be called from
// the snapshot thread, never the main thread. This returns the ID of the new
// snapshot, along with an error.
//
// ctx allows the work to be abandoned when the caller gives up; a
// context.Background() context (used by automatic snapshots) is never
// canceled. userFuture is non-nil only for user-triggered snapshots and
// arbitrates the race between a completed snapshot and a canceled context.
//
// On cancellation the in-flight snapshot is discarded: nothing is published
// as the latest stable snapshot, no log compaction happens, and every
// resource the snapshot store created is closed or deleted.
func (r *Raft) takeSnapshot(ctx context.Context, userFuture *userSnapshotFuture) (string, error) {
	defer metrics.MeasureSince([]string{"raft", "snapshot", "takeSnapshot"}, time.Now())

	// observeCancel flips the user future to its canceled state the first
	// time the context is observed done, closing the channel that unblocks
	// the sink wrapper. It is a no-op for automatic snapshots (nil
	// userFuture). It returns the context error for convenience.
	observeCancel := func() error {
		if userFuture != nil {
			userFuture.markCanceled()
		}
		return ctx.Err()
	}
	// observeContext checks the context without blocking and flips the user
	// future to canceled if it has expired since the last observation. This
	// covers cancellation that lands between select points, such as while
	// Persist is running. It reports whether the snapshot is now canceled.
	observeContext := func() bool {
		if userFuture == nil {
			return false
		}
		select {
		case <-ctx.Done():
			userFuture.markCanceled()
			return userFuture.canceled()
		default:
			return userFuture.canceled()
		}
	}
	// await blocks until the future's response fires, the context is
	// canceled, or Raft shuts down. It returns an error when the wait was
	// interrupted.
	await := func(respCh chan error) error {
		select {
		case <-respCh:
			return nil
		case <-ctx.Done():
			return observeCancel()
		case <-r.shutdownCh:
			return ErrRaftShutdown
		}
	}

	// Create a request for the FSM to perform a snapshot.
	snapReq := &reqSnapshotFuture{}
	snapReq.init()

	// Wait for dispatch or shutdown.
	select {
	case r.fsmSnapshotCh <- snapReq:
	case <-r.shutdownCh:
		return "", ErrRaftShutdown
	case <-ctx.Done():
		return "", observeCancel()
	}

	// Wait until we get a response, unless the caller gives up first. The
	// FSM goroutine owns snapReq until it responds, so on cancellation we
	// can not read its result here; the waiting goroutine instead releases
	// the FSMSnapshot once the response arrives.
	responded := make(chan struct{})
	var snapErr error
	go func() {
		snapErr = snapReq.Error()
		close(responded)
	}()
	select {
	case <-responded:
	case <-r.shutdownCh:
		return "", ErrRaftShutdown
	case <-ctx.Done():
		go func() {
			<-responded
			if snapErr == nil && snapReq.snapshot != nil {
				snapReq.snapshot.Release()
			}
		}()
		return "", observeCancel()
	}
	if snapErr != nil {
		if snapErr != ErrNothingNewToSnapshot {
			snapErr = fmt.Errorf("failed to start snapshot: %v", snapErr)
		}
		return "", snapErr
	}
	defer func() {
		if snapReq.snapshot != nil {
			snapReq.snapshot.Release()
		}
	}()

	// Make a request for the configurations and extract the committed info.
	// We have to use the future here to safely get this information since
	// it is owned by the main thread.
	configReq := &configurationsFuture{}
	configReq.ShutdownCh = r.shutdownCh
	configReq.init()
	select {
	case r.configurationsCh <- configReq:
	case <-r.shutdownCh:
		return "", ErrRaftShutdown
	case <-ctx.Done():
		return "", observeCancel()
	}
	if err := await(configReq.errCh); err != nil {
		return "", err
	}
	if err := configReq.Error(); err != nil {
		return "", err
	}
	committed := configReq.configurations.committed
	committedIndex := configReq.configurations.committedIndex

	// We don't support snapshots while there's a config change outstanding
	// since the snapshot doesn't have a means to represent this state. This
	// is a little weird because we need the FSM to apply an index that's
	// past the configuration change, even though the FSM itself doesn't see
	// the configuration changes. It should be ok in practice with normal
	// application traffic flowing through the FSM. If there's none of that
	// then it's not crucial that we snapshot, since there's not much going
	// on Raft-wise.
	if snapReq.index < committedIndex {
		return "", fmt.Errorf("cannot take snapshot now, wait until the configuration entry at %v has been applied (have applied %v)",
			committedIndex, snapReq.index)
	}

	// Check once more before creating any resources in the snapshot store.
	if observeContext() {
		return "", ctx.Err()
	}

	// Create a new snapshot.
	r.logger.Info("starting snapshot up to", "index", snapReq.index)
	start := time.Now()
	version := getSnapshotVersion(r.protocolVersion)
	sink, err := r.snapshots.Create(version, snapReq.index, snapReq.term, committed, committedIndex, r.trans)
	if err != nil {
		return "", fmt.Errorf("failed to create snapshot: %v", err)
	}
	metrics.MeasureSince([]string{"raft", "snapshot", "create"}, start)

	// Wrap the sink so that, for a cancelable snapshot, an FSM blocked in
	// Persist is unblocked as soon as the context is canceled, and so that
	// Close and Cancel can never both run against the underlying sink.
	var (
		wrapperCtx    context.Context
		wrapperCancel <-chan struct{}
	)
	if userFuture != nil {
		wrapperCtx = ctx
		wrapperCancel = userFuture.cancelCh()
	}
	sink = newCancellableSnapshotSink(sink, wrapperCtx, wrapperCancel, userFuture)

	// Try to persist the snapshot.
	start = time.Now()
	persistErr := snapReq.snapshot.Persist(sink)
	metrics.MeasureSince([]string{"raft", "snapshot", "persist"}, start)

	// A persist error means the snapshot can not succeed; discard the sink.
	if persistErr != nil {
		if cancelErr := sink.Cancel(); cancelErr != nil {
			r.logger.Error("failed to cancel snapshot after persist error", "error", cancelErr)
		}
		// Cancellation may have been the cause; report it when it won.
		if userFuture != nil && observeContext() {
			return "", ctx.Err()
		}
		return "", fmt.Errorf("failed to persist snapshot: %v", persistErr)
	}

	// Finalize the sink. The publication decision is arbitrated here through
	// tryCommit, so a context that has won makes Close fail without the
	// snapshot ever reaching the store; the sink is then discarded and the
	// stable snapshot and log compaction are left untouched.
	if err := sink.Close(); err != nil {
		if userFuture != nil && errors.Is(err, context.Canceled) {
			if cancelErr := sink.Cancel(); cancelErr != nil {
				r.logger.Error("failed to cancel snapshot after context cancellation", "error", cancelErr)
			}
			return "", ctx.Err()
		}
		return "", fmt.Errorf("failed to close snapshot: %v", err)
	}

	// Update the last stable snapshot info. This only runs once the snapshot
	// is fully committed, so a canceled snapshot never reaches it.
	r.setLastSnapshot(snapReq.index, snapReq.term)

	// Compact the logs. This only runs once the snapshot is fully committed,
	// so a canceled snapshot can never trigger log compaction.
	if err := r.compactLogs(snapReq.index); err != nil {
		return "", err
	}

	r.logger.Info("snapshot complete up to", "index", snapReq.index)
	return sink.ID(), nil
}

// compactLogsWithTrailing takes the last inclusive index of a snapshot,
// the lastLogIdx, and the trailingLogs and trims the logs that
// are no longer needed.
func (r *Raft) compactLogsWithTrailing(snapIdx uint64, lastLogIdx uint64, trailingLogs uint64) error {
	// Determine log ranges to compact
	minLog, err := r.logs.FirstIndex()
	if err != nil {
		return fmt.Errorf("failed to get first log index: %v", err)
	}

	// Check if we have enough logs to truncate
	// Use a consistent value for trailingLogs for the duration of this method
	// call to avoid surprising behaviour.
	if lastLogIdx <= trailingLogs {
		return nil
	}

	// Truncate up to the end of the snapshot, or `TrailingLogs`
	// back from the head, which ever is further back. This ensures
	// at least `TrailingLogs` entries, but does not allow logs
	// after the snapshot to be removed.
	maxLog := min(snapIdx, lastLogIdx-trailingLogs)

	if minLog > maxLog {
		r.logger.Info("no logs to truncate")
		return nil
	}

	r.logger.Info("compacting logs", "from", minLog, "to", maxLog)

	// Compact the logs
	if err := r.logs.DeleteRange(minLog, maxLog); err != nil {
		return fmt.Errorf("log compaction failed: %v", err)
	}
	return nil
}

// compactLogs takes the last inclusive index of a snapshot
// and trims the logs that are no longer needed.
func (r *Raft) compactLogs(snapIdx uint64) error {
	defer metrics.MeasureSince([]string{"raft", "compactLogs"}, time.Now())

	lastLogIdx, _ := r.getLastLog()
	trailingLogs := r.config().TrailingLogs

	return r.compactLogsWithTrailing(snapIdx, lastLogIdx, trailingLogs)
}

// removeOldLogs removes all old logs from the store. This is used for
// MonotonicLogStores after restore. Callers should verify that the store
// implementation is monotonic prior to calling.
func (r *Raft) removeOldLogs() error {
	defer metrics.MeasureSince([]string{"raft", "removeOldLogs"}, time.Now())

	lastLogIdx, err := r.logs.LastIndex()
	if err != nil {
		return fmt.Errorf("failed to get last log index: %w", err)
	}

	r.logger.Info("removing all old logs from log store")

	// call compactLogsWithTrailing with lastLogIdx for snapIdx since
	// it will take the lesser of lastLogIdx and snapIdx to figure out
	// the end for which to apply trailingLogs.
	return r.compactLogsWithTrailing(lastLogIdx, lastLogIdx, 0)
}
