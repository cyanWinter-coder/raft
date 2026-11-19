// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"
)

// Future is used to represent an action that may occur in the future.
type Future interface {
	// Error blocks until the future arrives and then returns the error status
	// of the future. This may be called any number of times - all calls will
	// return the same value, however is not OK to call this method twice
	// concurrently on the same Future instance.
	// Error will only return generic errors related to raft, such
	// as ErrLeadershipLost, or ErrRaftShutdown. Some operations, such as
	// ApplyLog, may also return errors from other methods.
	Error() error
}

// IndexFuture is used for future actions that can result in a raft log entry
// being created.
type IndexFuture interface {
	Future

	// Index holds the index of the newly applied log entry.
	// This must not be called until after the Error method has returned.
	Index() uint64
}

// ApplyFuture is used for Apply and can return the FSM response.
type ApplyFuture interface {
	IndexFuture

	// Response returns the FSM response as returned by the FSM.Apply method. This
	// must not be called until after the Error method has returned.
	// Note that if FSM.Apply returns an error, it will be returned by Response,
	// and not by the Error method, so it is always important to check Response
	// for errors from the FSM.
	Response() interface{}
}

// ConfigurationFuture is used for GetConfiguration and can return the
// latest configuration in use by Raft.
type ConfigurationFuture interface {
	IndexFuture

	// Configuration contains the latest configuration. This must
	// not be called until after the Error method has returned.
	Configuration() Configuration
}

// SnapshotFuture is used for waiting on a user-triggered snapshot to complete.
type SnapshotFuture interface {
	Future

	// Open is a function you can call to access the underlying snapshot and
	// its metadata. This must not be called until after the Error method
	// has returned.
	Open() (*SnapshotMeta, io.ReadCloser, error)
}

// LeadershipTransferFuture is used for waiting on a user-triggered leadership
// transfer to complete.
type LeadershipTransferFuture interface {
	Future
}

// errorFuture is used to return a static error.
type errorFuture struct {
	err error
}

func (e errorFuture) Error() error {
	return e.err
}

func (e errorFuture) Response() interface{} {
	return nil
}

func (e errorFuture) Index() uint64 {
	return 0
}

// deferError can be embedded to allow a future
// to provide an error in the future.
type deferError struct {
	err        error
	errCh      chan error
	responded  bool
	ShutdownCh chan struct{}
}

func (d *deferError) init() {
	d.errCh = make(chan error, 1)
}

func (d *deferError) Error() error {
	if d.err != nil {
		// Note that when we've received a nil error, this
		// won't trigger, but the channel is closed after
		// send so we'll still return nil below.
		return d.err
	}
	if d.errCh == nil {
		panic("waiting for response on nil channel")
	}
	select {
	case d.err = <-d.errCh:
	case <-d.ShutdownCh:
		d.err = ErrRaftShutdown
	}
	return d.err
}

func (d *deferError) respond(err error) {
	if d.errCh == nil {
		return
	}
	if d.responded {
		return
	}
	d.errCh <- err
	close(d.errCh)
	d.responded = true
}

// There are several types of requests that cause a configuration entry to
// be appended to the log. These are encoded here for leaderLoop() to process.
// This is internal to a single server.
type configurationChangeFuture struct {
	logFuture
	req configurationChangeRequest
}

// bootstrapFuture is used to attempt a live bootstrap of the cluster. See the
// Raft object's BootstrapCluster member function for more details.
type bootstrapFuture struct {
	deferError

	// configuration is the proposed bootstrap configuration to apply.
	configuration Configuration
}

// logFuture is used to apply a log entry and waits until
// the log is considered committed.
type logFuture struct {
	deferError
	log      Log
	response interface{}
	dispatch time.Time
}

func (l *logFuture) Response() interface{} {
	return l.response
}

func (l *logFuture) Index() uint64 {
	return l.log.Index
}

type shutdownFuture struct {
	raft *Raft
}

func (s *shutdownFuture) Error() error {
	if s.raft == nil {
		return nil
	}
	s.raft.waitShutdown()
	if closeable, ok := s.raft.trans.(WithClose); ok {
		_ = closeable.Close()
	}
	return nil
}

// userSnapshotFuture is used for waiting on a user-triggered snapshot to
// complete.
type userSnapshotFuture struct {
	deferError

	// ctx is the caller's context. A nil context is treated as one that is
	// never canceled, which keeps the behavior of Snapshot() (as opposed to
	// SnapshotWithContext()) unchanged.
	ctx context.Context

	// opener is a function used to open the snapshot. This is filled in
	// once the future returns with no error.
	opener func() (*SnapshotMeta, io.ReadCloser, error)

	// mu guards state and cancel. The arbitration between a completed
	// snapshot and a canceled context happens entirely under this lock, so
	// exactly one outcome can ever be reported.
	mu     sync.Mutex
	state  userSnapshotState
	cancel chan struct{}
}

// userSnapshotState tracks the lifecycle of a user-triggered snapshot.
type userSnapshotState int

const (
	// userSnapshotPending means the snapshot is still in flight and no
	// outcome has been decided.
	userSnapshotPending userSnapshotState = iota
	// userSnapshotCommitted means the snapshot finished successfully before
	// the context was canceled and may be published.
	userSnapshotCommitted
	// userSnapshotCanceled means the context was canceled before the
	// snapshot completed; any in-flight work must be discarded.
	userSnapshotCanceled
)

// initWithContext sets up the cancellation signal used by the snapshot
// goroutine. It must be called once before the future is dispatched. The
// context drives cancellation on its own, through context.AfterFunc, so the
// snapshot sink is notified even while the snapshot goroutine is blocked
// inside Persist and cannot poll the context.
func (u *userSnapshotFuture) initWithContext(ctx context.Context) {
	u.init()
	u.ctx = ctx
	u.cancel = make(chan struct{})
	context.AfterFunc(ctx, func() { _ = u.markCanceled() })
}

// done returns a channel that is closed once the context is canceled. It
// returns nil when there is no context, and a nil channel never fires in a
// select, so the automatic/non-cancelable path is unaffected.
func (u *userSnapshotFuture) done() <-chan struct{} {
	if u.ctx == nil {
		return nil
	}
	return u.ctx.Done()
}

// markCanceled records that the caller has given up. It returns true if the
// snapshot is now canceled and its work must be discarded, and false if the
// snapshot had already been committed and the cancellation is too late.
func (u *userSnapshotFuture) markCanceled() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.state != userSnapshotPending {
		return false
	}
	u.state = userSnapshotCanceled
	close(u.cancel)
	return true
}

// canceled reports whether the snapshot has been canceled.
func (u *userSnapshotFuture) canceled() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.state == userSnapshotCanceled
}

// cancelCh returns a channel that is closed when the snapshot is canceled.
// Unlike done(), it is never nil so it can be handed to snapshot sinks.
func (u *userSnapshotFuture) cancelCh() <-chan struct{} {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.cancel
}

// tryCommit records that the snapshot has finished successfully. It returns
// true only when success wins the race against cancellation and the snapshot
// may be published, and false when cancellation won and it must be discarded.
//
// Arbitration is a simple state transition under the mutex: cancellation is
// driven only by markCanceled, which is installed via context.AfterFunc and
// guaranteed by the context package to run once the context is canceled. The
// commit path and the cancellation path therefore serialize on the same
// mutex, and whichever reaches it first from a pending state wins. No context
// channel is polluted into this decision, so there is no check-then-act gap.
func (u *userSnapshotFuture) tryCommit() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.state != userSnapshotPending {
		return u.state == userSnapshotCommitted
	}
	u.state = userSnapshotCommitted
	return true
}

// Error blocks until the snapshot completes or, when a context was supplied,
// until that context is canceled. On cancellation it returns the context's
// error promptly without waiting for the in-flight snapshot work to finish;
// that work is discarded asynchronously and never published. Subsequent calls
// keep returning the same error.
func (u *userSnapshotFuture) Error() error {
	if u.err != nil {
		return u.err
	}
	if u.errCh == nil {
		panic("waiting for response on nil channel")
	}

	// Read the arbitrated state before blocking. A snapshot already committed
	// must keep resolving to success even if the context is canceled in the
	// gap between commitment and respond(); in that case we wait for the
	// response rather than surfacing the late context error.
	u.mu.Lock()
	committed := u.state == userSnapshotCommitted
	u.mu.Unlock()

	select {
	case u.err = <-u.errCh:
		return u.err
	case <-u.done():
		if committed {
			u.err = <-u.errCh
			return u.err
		}
		u.err = u.ctx.Err()
		return u.err
	}
}

// Open is a function you can call to access the underlying snapshot and its
// metadata.
func (u *userSnapshotFuture) Open() (*SnapshotMeta, io.ReadCloser, error) {
	if u.opener == nil {
		return nil, nil, fmt.Errorf("no snapshot available")
	}
	// Invalidate the opener so it can't get called multiple times,
	// which isn't generally safe.
	defer func() {
		u.opener = nil
	}()
	return u.opener()
}

// userRestoreFuture is used for waiting on a user-triggered restore of an
// external snapshot to complete.
type userRestoreFuture struct {
	deferError

	// meta is the metadata that belongs with the snapshot.
	meta *SnapshotMeta

	// reader is the interface to read the snapshot contents from.
	reader io.Reader
}

// reqSnapshotFuture is used for requesting a snapshot start.
// It is only used internally.
type reqSnapshotFuture struct {
	deferError

	// snapshot details provided by the FSM runner before responding
	index    uint64
	term     uint64
	snapshot FSMSnapshot
}

// restoreFuture is used for requesting an FSM to perform a
// snapshot restore. Used internally only.
type restoreFuture struct {
	deferError
	ID string
}

// verifyFuture is used to verify the current node is still
// the leader. This is to prevent a stale read.
type verifyFuture struct {
	deferError
	notifyCh   chan *verifyFuture
	quorumSize int
	votes      int
	voteLock   sync.Mutex
}

// leadershipTransferFuture is used to track the progress of a leadership
// transfer internally.
type leadershipTransferFuture struct {
	deferError

	ID      *ServerID
	Address *ServerAddress
}

// configurationsFuture is used to retrieve the current configurations. This is
// used to allow safe access to this information outside of the main thread.
type configurationsFuture struct {
	deferError
	configurations configurations
}

// Configuration returns the latest configuration in use by Raft.
func (c *configurationsFuture) Configuration() Configuration {
	return c.configurations.latest
}

// Index returns the index of the latest configuration in use by Raft.
func (c *configurationsFuture) Index() uint64 {
	return c.configurations.latestIndex
}

// vote is used to respond to a verifyFuture.
// This may block when responding on the notifyCh.
func (v *verifyFuture) vote(leader bool) {
	v.voteLock.Lock()
	defer v.voteLock.Unlock()

	// Guard against having notified already
	if v.notifyCh == nil {
		return
	}

	if leader {
		v.votes++
		if v.votes >= v.quorumSize {
			v.notifyCh <- v
			v.notifyCh = nil
		}
	} else {
		v.notifyCh <- v
		v.notifyCh = nil
	}
}

// appendFuture is used for waiting on a pipelined append
// entries RPC.
type appendFuture struct {
	deferError
	start time.Time
	args  *AppendEntriesRequest
	resp  *AppendEntriesResponse
}

func (a *appendFuture) Start() time.Time {
	return a.start
}

func (a *appendFuture) Request() *AppendEntriesRequest {
	return a.args
}

func (a *appendFuture) Response() *AppendEntriesResponse {
	return a.resp
}
