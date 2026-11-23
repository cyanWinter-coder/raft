// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// NewInmemAddr returns a new in-memory addr with
// a randomly generate UUID as the ID.
func NewInmemAddr() ServerAddress {
	return ServerAddress(generateUUID())
}

// inmemPipeline is used to pipeline requests for the in-mem transport.
type inmemPipeline struct {
	trans    *InmemTransport
	peer     *InmemTransport
	peerAddr ServerAddress

	doneCh       chan AppendFuture
	inprogressCh chan *inmemPipelineInflight

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.RWMutex

	// pending tracks every future the transport accepted but has not completed
	// yet. Close completes each of them exactly once with ErrPipelineShutdown,
	// mirroring the network transport's behaviour. pendingClosed is set under
	// pendingLock when Close takes the set, so a concurrent send either joins
	// the set (and is completed by Close) or is refused.
	pendingLock   sync.Mutex
	pending       map[*appendFuture]struct{}
	pendingClosed bool
}

type inmemPipelineInflight struct {
	future *appendFuture
	respCh <-chan RPCResponse
}

// InmemTransport Implements the Transport interface, to allow Raft to be
// tested in-memory without going over a network.
type InmemTransport struct {
	sync.RWMutex
	consumerCh chan RPC
	localAddr  ServerAddress
	peers      map[ServerAddress]*InmemTransport
	pipelines  []*inmemPipeline
	timeout    time.Duration
}

// NewInmemTransportWithTimeout is used to initialize a new transport and
// generates a random local address if none is specified. The given timeout
// will be used to decide how long to wait for a connected peer to process the
// RPCs that we're sending it. See also Connect() and Consumer().
func NewInmemTransportWithTimeout(addr ServerAddress, timeout time.Duration) (ServerAddress, *InmemTransport) {
	if string(addr) == "" {
		addr = NewInmemAddr()
	}
	trans := &InmemTransport{
		consumerCh: make(chan RPC, 16),
		localAddr:  addr,
		peers:      make(map[ServerAddress]*InmemTransport),
		timeout:    timeout,
	}
	return addr, trans
}

// NewInmemTransport is used to initialize a new transport
// and generates a random local address if none is specified
func NewInmemTransport(addr ServerAddress) (ServerAddress, *InmemTransport) {
	return NewInmemTransportWithTimeout(addr, 500*time.Millisecond)
}

// SetHeartbeatHandler is used to set optional fast-path for
// heartbeats, not supported for this transport.
func (i *InmemTransport) SetHeartbeatHandler(cb func(RPC)) {
}

// Consumer implements the Transport interface.
func (i *InmemTransport) Consumer() <-chan RPC {
	return i.consumerCh
}

// LocalAddr implements the Transport interface.
func (i *InmemTransport) LocalAddr() ServerAddress {
	return i.localAddr
}

// AppendEntriesPipeline returns an interface that can be used to pipeline
// AppendEntries requests.
func (i *InmemTransport) AppendEntriesPipeline(id ServerID, target ServerAddress) (AppendPipeline, error) {
	i.Lock()
	defer i.Unlock()

	peer, ok := i.peers[target]
	if !ok {
		return nil, fmt.Errorf("failed to connect to peer: %v", target)
	}
	pipeline := newInmemPipeline(i, peer, target)
	i.pipelines = append(i.pipelines, pipeline)
	return pipeline, nil
}

// AppendEntries implements the Transport interface.
func (i *InmemTransport) AppendEntries(id ServerID, target ServerAddress, args *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	rpcResp, err := i.makeRPC(target, args, nil, i.timeout)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*AppendEntriesResponse)
	*resp = *out
	return nil
}

// RequestVote implements the Transport interface.
func (i *InmemTransport) RequestVote(id ServerID, target ServerAddress, args *RequestVoteRequest, resp *RequestVoteResponse) error {
	rpcResp, err := i.makeRPC(target, args, nil, i.timeout)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*RequestVoteResponse)
	*resp = *out
	return nil
}

func (i *InmemTransport) RequestPreVote(id ServerID, target ServerAddress, args *RequestPreVoteRequest, resp *RequestPreVoteResponse) error {
	rpcResp, err := i.makeRPC(target, args, nil, i.timeout)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*RequestPreVoteResponse)
	*resp = *out
	return nil
}

// InstallSnapshot implements the Transport interface.
func (i *InmemTransport) InstallSnapshot(id ServerID, target ServerAddress, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.Reader) error {
	rpcResp, err := i.makeRPC(target, args, data, 10*i.timeout)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*InstallSnapshotResponse)
	*resp = *out
	return nil
}

// TimeoutNow implements the Transport interface.
func (i *InmemTransport) TimeoutNow(id ServerID, target ServerAddress, args *TimeoutNowRequest, resp *TimeoutNowResponse) error {
	rpcResp, err := i.makeRPC(target, args, nil, 10*i.timeout)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*TimeoutNowResponse)
	*resp = *out
	return nil
}

func (i *InmemTransport) makeRPC(target ServerAddress, args interface{}, r io.Reader, timeout time.Duration) (rpcResp RPCResponse, err error) {
	i.RLock()
	peer, ok := i.peers[target]
	i.RUnlock()

	if !ok {
		err = fmt.Errorf("failed to connect to peer: %v", target)
		return
	}

	// Send the RPC over
	respCh := make(chan RPCResponse, 1)
	req := RPC{
		Command:  args,
		Reader:   r,
		RespChan: respCh,
	}
	select {
	case peer.consumerCh <- req:
	case <-time.After(timeout):
		err = fmt.Errorf("send timed out")
		return
	}

	// Wait for a response
	select {
	case rpcResp = <-respCh:
		if rpcResp.Error != nil {
			err = rpcResp.Error
		}
	case <-time.After(timeout):
		err = fmt.Errorf("command timed out")
	}
	return
}

// EncodePeer implements the Transport interface.
func (i *InmemTransport) EncodePeer(id ServerID, p ServerAddress) []byte {
	return []byte(p)
}

// DecodePeer implements the Transport interface.
func (i *InmemTransport) DecodePeer(buf []byte) ServerAddress {
	return ServerAddress(buf)
}

// Connect is used to connect this transport to another transport for
// a given peer name. This allows for local routing.
func (i *InmemTransport) Connect(peer ServerAddress, t Transport) {
	trans := t.(*InmemTransport)
	i.Lock()
	defer i.Unlock()
	i.peers[peer] = trans
}

// Disconnect is used to remove the ability to route to a given peer.
func (i *InmemTransport) Disconnect(peer ServerAddress) {
	i.Lock()
	defer i.Unlock()
	delete(i.peers, peer)

	// Disconnect any pipelines
	n := len(i.pipelines)
	for idx := 0; idx < n; idx++ {
		if i.pipelines[idx].peerAddr == peer {
			_ = i.pipelines[idx].Close()
			i.pipelines[idx], i.pipelines[n-1] = i.pipelines[n-1], nil
			idx--
			n--
		}
	}
	i.pipelines = i.pipelines[:n]
}

// DisconnectAll is used to remove all routes to peers.
func (i *InmemTransport) DisconnectAll() {
	i.Lock()
	defer i.Unlock()
	i.peers = make(map[ServerAddress]*InmemTransport)

	// Handle pipelines
	for _, pipeline := range i.pipelines {
		_ = pipeline.Close()
	}
	i.pipelines = nil
}

// Close is used to permanently disable the transport
func (i *InmemTransport) Close() error {
	i.DisconnectAll()
	return nil
}

func newInmemPipeline(trans *InmemTransport, peer *InmemTransport, addr ServerAddress) *inmemPipeline {
	i := &inmemPipeline{
		trans:        trans,
		peer:         peer,
		peerAddr:     addr,
		doneCh:       make(chan AppendFuture, 16),
		inprogressCh: make(chan *inmemPipelineInflight, 16),
		shutdownCh:   make(chan struct{}),
		pending:      make(map[*appendFuture]struct{}),
	}
	go i.decodeResponses()
	return i
}

// trackFuture records a future as in flight so Close can complete it if the
// connection goes away before its response arrives. It reports false when the
// pipeline is already shutting down, in which case the caller must not send.
// The pending set and the closed flag share pendingLock with Close, so a
// future is either flushed by Close or refused here.
func (i *inmemPipeline) trackFuture(future *appendFuture) bool {
	i.pendingLock.Lock()
	defer i.pendingLock.Unlock()
	if i.pendingClosed {
		return false
	}
	i.pending[future] = struct{}{}
	return true
}

// claimFuture removes a future from the pending set, transferring ownership of
// its completion to the caller. It returns false when the future was already
// completed by Close (or a previous claim), in which case the caller must not
// touch its response buffer. Claiming before decoding guarantees that Close's
// flush and the decoder can never both write the same future.
func (i *inmemPipeline) claimFuture(future *appendFuture) bool {
	i.pendingLock.Lock()
	defer i.pendingLock.Unlock()
	if _, ok := i.pending[future]; !ok {
		return false
	}
	delete(i.pending, future)
	return true
}

// deliverFuture forwards a completed future to the consumer channel, giving up
// once the pipeline shuts down. The future is already completed, so its waiter
// does not block.
func (i *inmemPipeline) deliverFuture(future *appendFuture) bool {
	select {
	case i.doneCh <- future:
		return true
	case <-i.shutdownCh:
		return false
	}
}

func (i *inmemPipeline) decodeResponses() {
	timeout := i.trans.timeout
	for {
		select {
		case inp := <-i.inprogressCh:
			var timeoutCh <-chan time.Time
			if timeout > 0 {
				timeoutCh = time.After(timeout)
			}

			select {
			case rpcResp := <-inp.respCh:
				// Claim the future before copying into its response buffer. If
				// Close already completed it, stop touching it.
				if !i.claimFuture(inp.future) {
					return
				}
				// Copy the result back
				if resp, ok := rpcResp.Response.(*AppendEntriesResponse); ok {
					*inp.future.resp = *resp
				}
				inp.future.respond(rpcResp.Error)
				if !i.deliverFuture(inp.future) {
					return
				}

			case <-timeoutCh:
				if !i.claimFuture(inp.future) {
					return
				}
				inp.future.respond(fmt.Errorf("command timed out"))
				if !i.deliverFuture(inp.future) {
					return
				}

			case <-i.shutdownCh:
				return
			}
		case <-i.shutdownCh:
			return
		}
	}
}

func (i *inmemPipeline) AppendEntries(args *AppendEntriesRequest, resp *AppendEntriesResponse) (AppendFuture, error) {
	// Create a new future
	future := &appendFuture{
		start: time.Now(),
		args:  args,
		resp:  resp,
	}
	future.init()

	// Handle a timeout
	var timeout <-chan time.Time
	if i.trans.timeout > 0 {
		timeout = time.After(i.trans.timeout)
	}

	// Send the RPC over
	respCh := make(chan RPCResponse, 1)
	rpc := RPC{
		Command:  args,
		RespChan: respCh,
	}

	// Check if we have been already shutdown, otherwise the random choose
	// made by select statement below might pick consumerCh even if
	// shutdownCh was closed.
	i.shutdownLock.RLock()
	shutdown := i.shutdown
	i.shutdownLock.RUnlock()
	if shutdown {
		return nil, ErrPipelineShutdown
	}

	// Track the future before enqueueing so Close cannot miss a request whose
	// response is still in progress.
	if !i.trackFuture(future) {
		return nil, ErrPipelineShutdown
	}

	select {
	case i.peer.consumerCh <- rpc:
	case <-timeout:
		// Claim then complete only if we still own the future: Close may have
		// beaten us here and already responded it.
		if i.claimFuture(future) {
			future.respond(fmt.Errorf("command enqueue timeout"))
		}
		return nil, fmt.Errorf("command enqueue timeout")
	case <-i.shutdownCh:
		if i.claimFuture(future) {
			future.respond(ErrPipelineShutdown)
		}
		return nil, ErrPipelineShutdown
	}

	// Send to be decoded
	select {
	case i.inprogressCh <- &inmemPipelineInflight{future, respCh}:
		return future, nil
	case <-i.shutdownCh:
		if i.claimFuture(future) {
			future.respond(ErrPipelineShutdown)
		}
		return nil, ErrPipelineShutdown
	}
}

func (i *inmemPipeline) Consumer() <-chan AppendFuture {
	return i.doneCh
}

func (i *inmemPipeline) Close() error {
	i.shutdownLock.Lock()
	if i.shutdown {
		i.shutdownLock.Unlock()
		return nil
	}
	i.shutdown = true
	i.shutdownLock.Unlock()

	// Complete every in-flight future exactly once and publish them to the
	// consumer so a draining session observes the terminal result promptly.
	// Publishing happens before shutdownCh is closed; delivery is
	// non-blocking so a consumer that is not reading cannot wedge Close. This
	// mirrors the network pipeline so both transports present the same close
	// semantics.
	i.pendingLock.Lock()
	pending := i.pending
	i.pending = make(map[*appendFuture]struct{})
	i.pendingClosed = true
	i.pendingLock.Unlock()
	for future := range pending {
		future.respond(ErrPipelineShutdown)
	}
	for future := range pending {
		select {
		case i.doneCh <- future:
		default:
		}
	}

	i.shutdownLock.Lock()
	close(i.shutdownCh)
	i.shutdownLock.Unlock()
	return nil
}
