// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-metrics"
	"github.com/hashicorp/go-msgpack/v2/codec"
)

const (
	rpcAppendEntries uint8 = iota
	rpcRequestVote
	rpcInstallSnapshot
	rpcTimeoutNow
	rpcRequestPreVote

	// DefaultTimeoutScale is the default TimeoutScale in a NetworkTransport.
	DefaultTimeoutScale = 256 * 1024 // 256KB

	// DefaultMaxRPCsInFlight is the default value used for pipelining configuration
	// if a zero value is passed. See https://github.com/hashicorp/raft/pull/541
	// for rationale. Note, if this is changed we should update the doc comments
	// below for NetworkTransportConfig.MaxRPCsInFlight.
	DefaultMaxRPCsInFlight = 2

	// connReceiveBufferSize is the size of the buffer we will use for reading RPC requests into
	// on followers
	connReceiveBufferSize = 256 * 1024 // 256KB

	// connSendBufferSize is the size of the buffer we will use for sending RPC request data from
	// the leader to followers.
	connSendBufferSize = 256 * 1024 // 256KB

	// minInFlightForPipelining is a property of our current pipelining
	// implementation and must not be changed unless we change the invariants of
	// that implementation. Roughly speaking even with a zero-length in-flight
	// buffer we still allow 2 requests to be in-flight before we block because we
	// only block after sending and the receiving go-routine always unblocks the
	// chan right after first send. This is a constant just to provide context
	// rather than a magic number in a few places we have to check invariants to
	// avoid panics etc.
	minInFlightForPipelining = 2

	// DefaultReconnectMinDelay is the first backoff delay used when dialing a
	// peer fails.
	DefaultReconnectMinDelay = 5 * time.Millisecond

	// DefaultReconnectMaxDelay caps the exponential dial-backoff delay.
	DefaultReconnectMaxDelay = time.Second

	// DefaultReconnectMaxRetries controls how many additional dial attempts a
	// transport RPC makes after the first failure. The zero configured value
	// uses this default; a negative value disables internal retries.
	DefaultReconnectMaxRetries = 2

	// DefaultCloseTimeout bounds how long Close waits for already accepted
	// requests before forcibly closing their connections.
	DefaultCloseTimeout = 2 * time.Second
)

var (
	// ErrTransportShutdown is returned when operations on a transport are
	// invoked after it's been terminated.
	ErrTransportShutdown = errors.New("transport shutdown")

	// ErrPipelineShutdown is returned when the pipeline is closed.
	ErrPipelineShutdown = errors.New("append pipeline closed")

	// ErrStaleResponse is returned when a response's term/sequence correlation
	// does not match the outstanding request. The mismatched frame is
	// discarded and must never update Raft state.
	ErrStaleResponse = errors.New("stale RPC response discarded")
)

// NetworkTransport provides a network based transport that can be
// used to communicate with Raft on remote machines. It requires
// an underlying stream layer to provide a stream abstraction, which can
// be simple TCP, TLS, etc.
//
// This transport is very simple and lightweight. Each RPC request is
// framed by sending a byte that indicates the message type, followed
// by the MsgPack encoded request.
//
// The response is an error string followed by the response object,
// both are encoded using MsgPack.
//
// InstallSnapshot is special, in that after the RPC request we stream
// the entire state. That socket is not re-used as the connection state
// is not known if there is an error.
type NetworkTransport struct {
	connPool     map[ServerAddress][]*netConn
	connPoolLock sync.Mutex

	consumeCh chan RPC

	heartbeatFn     func(RPC)
	heartbeatFnLock sync.Mutex

	logger hclog.Logger

	maxPool     int
	maxInFlight int

	// rpcSeq is a transport-wide monotonic request sequence. Together with the
	// request term it correlates responses and allows stale frames to be
	// identified even when a connection delivers responses out of order.
	rpcSeq atomic.Uint64

	// inflight tracks request work that Close should drain: outbound RPCs and
	// inbound command handlers. Long-lived accept loops and idle conns are not
	// included because they are not requests.
	inflight sync.WaitGroup

	activeConns     map[net.Conn]struct{}
	activeConnsLock sync.Mutex

	// listenReadyCh, when initialized, is closed once the accept loop has
	// reached its first Accept call. Constructors wait for it so a caller
	// cannot dial and reset streams before Accept has pinned the current
	// context generation.
	listenReadyCh chan struct{}

	closeTimeout    time.Duration
	shutdownTimeout time.Duration

	dialMinBackoff time.Duration
	dialMaxBackoff time.Duration
	dialMaxRetries int

	serverAddressLock     sync.RWMutex
	serverAddressProvider ServerAddressProvider

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	forceCloseCh   chan struct{}
	forceCloseOnce sync.Once

	stream StreamLayer

	// streamCtx is used to cancel existing connection handlers.
	streamCtx     context.Context
	streamCancel  context.CancelFunc
	streamCtxLock sync.RWMutex

	timeout      time.Duration
	TimeoutScale int

	msgpackUseNewTimeFormat bool
}

// NetworkTransportConfig encapsulates configuration for the network transport layer.
type NetworkTransportConfig struct {
	// ServerAddressProvider is used to override the target address when establishing a connection to invoke an RPC
	ServerAddressProvider ServerAddressProvider

	Logger hclog.Logger

	// Dialer
	Stream StreamLayer

	// MaxPool controls how many connections we will pool
	MaxPool int

	// MaxRPCsInFlight controls the pipelining "optimization" when replicating
	// entries to followers.
	//
	// Setting this to 1 explicitly disables pipelining since no overlapping of
	// request processing is allowed. If set to 1 the pipelining code path is
	// skipped entirely and every request is entirely synchronous.
	//
	// If zero is set (or left as default), DefaultMaxRPCsInFlight is used which
	// is currently 2. A value of 2 overlaps the preparation and sending of the
	// next request while waiting for the previous response, but avoids additional
	// queuing.
	//
	// Historically this was internally fixed at (effectively) 130 however
	// performance testing has shown that in practice the pipelining optimization
	// combines badly with batching and actually has a very large negative impact
	// on commit latency when throughput is high, whilst having very little
	// benefit on latency or throughput in any other case! See
	// [#541](https://github.com/hashicorp/raft/pull/541) for more analysis of the
	// performance impacts.
	//
	// Increasing this beyond 2 is likely to be beneficial only in very
	// high-latency network conditions. HashiCorp doesn't recommend using our own
	// products this way.
	//
	// To maintain the behavior from before version 1.4.1 exactly, set this to
	// 130. The old internal constant was 128 but was used directly as a channel
	// buffer size. Since we send before blocking on the channel and unblock the
	// channel as soon as the receiver is done with the earliest outstanding
	// request, even an unbuffered channel (buffer=0) allows one request to be
	// sent while waiting for the previous one (i.e. 2 inflight). so the old
	// buffer actually allowed 130 RPCs to be inflight at once.
	MaxRPCsInFlight int

	// Timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
	// the timeout by (SnapshotSize / TimeoutScale).
	Timeout time.Duration

	// CloseTimeout bounds the graceful drain performed by Close. Already
	// accepted requests are given this long to finish; after that their
	// connections are forcibly closed. Zero uses DefaultCloseTimeout.
	CloseTimeout time.Duration

	// ShutdownTimeout is an alias for CloseTimeout used by callers that name
	// the graceful shutdown budget differently.
	ShutdownTimeout time.Duration

	// ReconnectMinDelay and ReconnectMaxDelay configure exponential backoff
	// between failed dial attempts. Zero values use
	// DefaultReconnectMinDelay and DefaultReconnectMaxDelay.
	ReconnectMinDelay time.Duration
	ReconnectMaxDelay time.Duration

	// ReconnectMaxRetries is the number of additional dial attempts after the
	// first failure. Zero uses DefaultReconnectMaxRetries; a negative value
	// disables transport-level retries and preserves the historical one-dial
	// behavior.
	ReconnectMaxRetries int

	// MsgpackUseNewTimeFormat when set to true, force the underlying msgpack
	// codec to use the new format of time.Time when encoding (used in
	// go-msgpack v1.1.5 by default). Decoding is not affected, as all
	// go-msgpack v2.1.0+ decoders know how to decode both formats.
	MsgpackUseNewTimeFormat bool
}

// ServerAddressProvider is a target address to which we invoke an RPC when establishing a connection
type ServerAddressProvider interface {
	ServerAddr(id ServerID) (ServerAddress, error)
}

// StreamLayer is used with the NetworkTransport to provide
// the low level stream abstraction.
type StreamLayer interface {
	net.Listener

	// Dial is used to create a new outgoing connection
	Dial(address ServerAddress, timeout time.Duration) (net.Conn, error)
}

// rpcCorrelation is encoded immediately after the RPC type byte on requests
// and at the start of each response. The sequence is transport-wide and
// monotonic; Term is the Raft term carried by the request. The pair lets a
// receiver reject a frame left behind by an old connection attempt or
// delivered after its waiter was retired.
type rpcCorrelation struct {
	Term     uint64
	Sequence uint64
}

// nextSequence returns the next transport-wide request sequence.
func (n *NetworkTransport) nextSequence() uint64 {
	return n.rpcSeq.Add(1)
}

// requestTerm extracts the Raft term from the supported request types.
func requestTerm(args interface{}) uint64 {
	switch req := args.(type) {
	case *AppendEntriesRequest:
		return req.Term
	case *RequestVoteRequest:
		return req.Term
	case *RequestPreVoteRequest:
		return req.Term
	case *InstallSnapshotRequest:
		return req.Term
	default:
		return 0
	}
}

type netConn struct {
	target ServerAddress
	conn   net.Conn
	w      *bufio.Writer
	dec    *codec.Decoder
	enc    *codec.Encoder

	// writeMu serializes complete framed writes. Heartbeats and log RPCs may
	// share a connection and can be encoded from different goroutines.
	writeMu sync.Mutex

	closeOnce sync.Once
	onRelease func(*netConn)
}

func (n *netConn) Release() error {
	var err error
	n.closeOnce.Do(func() {
		err = n.conn.Close()
		if n.onRelease != nil {
			n.onRelease(n)
		}
	})
	return err
}

type pendingAppend struct {
	future *appendFuture
	corr   rpcCorrelation
}

type netPipeline struct {
	conn  *netConn
	trans *NetworkTransport

	doneCh       chan AppendFuture
	inprogressCh chan *appendFuture

	// pending correlates response frames by request sequence. It allows
	// responses to be consumed correctly if a shared connection schedules
	// heartbeats and log appends independently.
	pending   map[uint64]*pendingAppend
	pendingMu sync.Mutex

	// slots bounds the number of accepted but unfinished requests.
	slots chan struct{}

	// pendingWG tracks futures registered with pending so Close can wait for
	// natural completion before forcibly disconnecting.
	pendingWG sync.WaitGroup

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// NewNetworkTransportWithConfig creates a new network transport with the given config struct
func NewNetworkTransportWithConfig(
	config *NetworkTransportConfig,
) *NetworkTransport {
	if config.Logger == nil {
		config.Logger = hclog.New(&hclog.LoggerOptions{
			Name:   "raft-net",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}
	maxInFlight := config.MaxRPCsInFlight
	if maxInFlight == 0 {
		// Default zero value
		maxInFlight = DefaultMaxRPCsInFlight
	}
	closeTimeout := config.CloseTimeout
	if closeTimeout == 0 {
		closeTimeout = config.ShutdownTimeout
	}
	if closeTimeout == 0 {
		closeTimeout = DefaultCloseTimeout
	}
	dialMinBackoff := config.ReconnectMinDelay
	if dialMinBackoff == 0 {
		dialMinBackoff = DefaultReconnectMinDelay
	}
	dialMaxBackoff := config.ReconnectMaxDelay
	if dialMaxBackoff == 0 {
		dialMaxBackoff = DefaultReconnectMaxDelay
	}
	dialMaxRetries := config.ReconnectMaxRetries
	if dialMaxRetries == 0 {
		dialMaxRetries = DefaultReconnectMaxRetries
	}

	trans := &NetworkTransport{
		connPool:                make(map[ServerAddress][]*netConn),
		consumeCh:               make(chan RPC),
		logger:                  config.Logger,
		maxPool:                 config.MaxPool,
		maxInFlight:             maxInFlight,
		shutdownCh:              make(chan struct{}),
		forceCloseCh:            make(chan struct{}),
		stream:                  config.Stream,
		timeout:                 config.Timeout,
		closeTimeout:            closeTimeout,
		dialMinBackoff:          dialMinBackoff,
		dialMaxBackoff:          dialMaxBackoff,
		dialMaxRetries:          dialMaxRetries,
		activeConns:             make(map[net.Conn]struct{}),
		listenReadyCh:           make(chan struct{}),
		TimeoutScale:            DefaultTimeoutScale,
		serverAddressProvider:   config.ServerAddressProvider,
		msgpackUseNewTimeFormat: config.MsgpackUseNewTimeFormat,
	}

	// Create the connection context and then start our listener.
	trans.setupStreamContext()
	go trans.listen()
	if trans.listenReadyCh != nil {
		<-trans.listenReadyCh
	}

	return trans
}

// NewNetworkTransport creates a new network transport with the given dialer
// and listener. The maxPool controls how many connections we will pool. The
// timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
// the timeout by (SnapshotSize / TimeoutScale).
func NewNetworkTransport(
	stream StreamLayer,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer,
) *NetworkTransport {
	if logOutput == nil {
		logOutput = os.Stderr
	}
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "raft-net",
		Output: logOutput,
		Level:  hclog.DefaultLevel,
	})
	config := &NetworkTransportConfig{Stream: stream, MaxPool: maxPool, Timeout: timeout, Logger: logger}
	return NewNetworkTransportWithConfig(config)
}

// NewNetworkTransportWithLogger creates a new network transport with the given logger, dialer
// and listener. The maxPool controls how many connections we will pool. The
// timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
// the timeout by (SnapshotSize / TimeoutScale).
func NewNetworkTransportWithLogger(
	stream StreamLayer,
	maxPool int,
	timeout time.Duration,
	logger hclog.Logger,
) *NetworkTransport {
	config := &NetworkTransportConfig{Stream: stream, MaxPool: maxPool, Timeout: timeout, Logger: logger}
	return NewNetworkTransportWithConfig(config)
}

// setupStreamContext is used to create a new stream context. This should be
// called with the stream lock held.
func (n *NetworkTransport) setupStreamContext() {
	ctx, cancel := context.WithCancel(context.Background())
	n.streamCtx = ctx
	n.streamCancel = cancel
}

// getStreamContext is used retrieve the current stream context.
func (n *NetworkTransport) getStreamContext() context.Context {
	n.streamCtxLock.RLock()
	defer n.streamCtxLock.RUnlock()
	return n.streamCtx
}

// SetHeartbeatHandler is used to set up a heartbeat handler
// as a fast-pass. This is to avoid head-of-line blocking from
// disk IO.
func (n *NetworkTransport) SetHeartbeatHandler(cb func(rpc RPC)) {
	n.heartbeatFnLock.Lock()
	defer n.heartbeatFnLock.Unlock()
	n.heartbeatFn = cb
}

// CloseStreams closes the current streams.
func (n *NetworkTransport) CloseStreams() {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	// Close all the connections in the connection pool and then remove their
	// entry.
	for k, e := range n.connPool {
		for _, conn := range e {
			_ = conn.Release()
		}

		delete(n.connPool, k)
	}

	// Also close active inbound/outbound streams. Idle pooled connections are
	// in both maps and Release/Close is idempotent; tracked active conns are
	// the ones that would otherwise only notice the canceled context after
	// their next frame boundary.
	n.activeConnsLock.Lock()
	for conn := range n.activeConns {
		_ = conn.Close()
	}
	n.activeConnsLock.Unlock()

	// Cancel the existing connections and create a new context. Both these
	// operations must always be done with the lock held otherwise we can create
	// connection handlers that are holding a context that will never be
	// cancelable.
	n.streamCtxLock.Lock()
	n.streamCancel()
	n.setupStreamContext()
	n.streamCtxLock.Unlock()
}

// Close is used to stop the network transport. It stops accepting new RPCs,
// waits for in-flight requests up to the configured CloseTimeout, and then
// forcibly closes tracked connections so blocked reads/writes cannot wedge
// shutdown forever.
func (n *NetworkTransport) Close() error {
	n.shutdownLock.Lock()
	if n.shutdown {
		n.shutdownLock.Unlock()
		return nil
	}

	close(n.shutdownCh)
	n.shutdown = true
	closeTimeout := n.gracefulCloseTimeout()
	streamCancel := n.streamCancel
	n.shutdownLock.Unlock()

	// Stop accepting new connections. Closing the stream layer closes the
	// listener for the built-in TCP transport.
	if n.stream != nil {
		_ = n.stream.Close()
	}
	if streamCancel != nil {
		streamCancel()
	}

	done := make(chan struct{})
	go func() {
		n.inflight.Wait()
		close(done)
	}()

	timer := time.NewTimer(closeTimeout)
	defer timer.Stop()
	select {
	case <-done:
		n.closeAllConns()
		return nil
	case <-timer.C:
		n.loggerWarn("timed out waiting for in-flight RPCs; forcibly closing connections",
			"timeout", closeTimeout)
		n.forceCloseOnce.Do(func() { close(n.forceCloseCh) })
		n.closeAllConns()
		return nil
	}
}

func (n *NetworkTransport) gracefulCloseTimeout() time.Duration {
	if n.closeTimeout > 0 {
		return n.closeTimeout
	}
	if n.shutdownTimeout > 0 {
		return n.shutdownTimeout
	}
	return DefaultCloseTimeout
}

// loggerWarn logs without panicking when a transport value was created as a
// zero-valued test fixture rather than through a constructor.
func (n *NetworkTransport) loggerWarn(msg string, args ...interface{}) {
	if n.logger != nil {
		n.logger.Warn(msg, args...)
	}
}

// beginRequest atomically rejects requests after Close and records an in-flight
// request. The shutdown lock serializes the shutdown flag against WaitGroup
// additions, avoiding a WaitGroup Add/Wait race.
func (n *NetworkTransport) beginRequest() bool {
	n.shutdownLock.Lock()
	defer n.shutdownLock.Unlock()
	if n.shutdown {
		return false
	}
	n.inflight.Add(1)
	return true
}

func (n *NetworkTransport) trackRawConn(conn net.Conn) {
	if conn == nil {
		return
	}
	n.activeConnsLock.Lock()
	if n.activeConns == nil {
		n.activeConns = make(map[net.Conn]struct{})
	}
	n.activeConns[conn] = struct{}{}
	n.activeConnsLock.Unlock()
}

func (n *NetworkTransport) untrackRawConn(conn net.Conn) {
	if conn == nil {
		return
	}
	n.activeConnsLock.Lock()
	delete(n.activeConns, conn)
	n.activeConnsLock.Unlock()
}

func (n *NetworkTransport) trackNetConn(conn *netConn) {
	n.trackRawConn(conn.conn)
	conn.onRelease = func(released *netConn) {
		n.untrackRawConn(released.conn)
	}
}

// closeAllConns forcibly closes every tracked raw connection. It is safe to
// call during shutdown and concurrent with ordinary Release calls.
func (n *NetworkTransport) closeAllConns() {
	n.activeConnsLock.Lock()
	conns := make([]net.Conn, 0, len(n.activeConns))
	for conn := range n.activeConns {
		conns = append(conns, conn)
	}
	n.activeConnsLock.Unlock()

	for _, conn := range conns {
		_ = conn.Close()
	}

	n.connPoolLock.Lock()
	n.connPool = make(map[ServerAddress][]*netConn)
	n.connPoolLock.Unlock()
}

// Consumer implements the Transport interface.
func (n *NetworkTransport) Consumer() <-chan RPC {
	return n.consumeCh
}

// LocalAddr implements the Transport interface.
func (n *NetworkTransport) LocalAddr() ServerAddress {
	return ServerAddress(n.stream.Addr().String())
}

// IsShutdown is used to check if the transport is shutdown.
func (n *NetworkTransport) IsShutdown() bool {
	select {
	case <-n.shutdownCh:
		return true
	default:
		return false
	}
}

// getExistingConn is used to grab a pooled connection.
func (n *NetworkTransport) getPooledConn(target ServerAddress) *netConn {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	conns, ok := n.connPool[target]
	if !ok || len(conns) == 0 {
		return nil
	}

	var conn *netConn
	num := len(conns)
	conn, conns[num-1] = conns[num-1], nil
	n.connPool[target] = conns[:num-1]
	return conn
}

// getConnFromAddressProvider returns a connection from the server address provider if available, or defaults to a connection using the target server address
func (n *NetworkTransport) getConnFromAddressProvider(id ServerID, target ServerAddress) (*netConn, error) {
	address := n.getProviderAddressOrFallback(id, target)
	return n.getConn(address)
}

func (n *NetworkTransport) getProviderAddressOrFallback(id ServerID, target ServerAddress) ServerAddress {
	n.serverAddressLock.RLock()
	defer n.serverAddressLock.RUnlock()
	if n.serverAddressProvider != nil {
		serverAddressOverride, err := n.serverAddressProvider.ServerAddr(id)
		if err != nil {
			n.logger.Warn("unable to get address for server, using fallback address", "id", id, "fallback", target, "error", err)
		} else {
			return serverAddressOverride
		}
	}
	return target
}

// getConn is used to get a connection from the pool. If no idle connection is
// available it dials, retrying failed connection establishments with capped
// exponential backoff. Retries stay local to the caller's peer goroutine, so a
// slow or unavailable node cannot prevent RPCs to other nodes.
func (n *NetworkTransport) getConn(target ServerAddress) (*netConn, error) {
	// Check for a pooled conn
	if conn := n.getPooledConn(target); conn != nil {
		return conn, nil
	}

	maxRetries := n.dialMaxRetries
	if maxRetries == 0 {
		// A zero transport can be created directly in tests; preserve the
		// production default there too.
		maxRetries = DefaultReconnectMaxRetries
	}

	var (
		conn net.Conn
		err  error
	)
	for attempt := 0; ; attempt++ {
		// Another goroutine may have returned an idle connection while we were
		// backing off.
		if pooled := n.getPooledConn(target); pooled != nil {
			return pooled, nil
		}

		// Dial a new connection
		conn, err = n.stream.Dial(target, n.timeout)
		if err == nil {
			break
		}

		if n.IsShutdown() {
			return nil, ErrTransportShutdown
		}
		if maxRetries < 0 || attempt >= maxRetries {
			return nil, err
		}

		delay := dialBackoffDelay(n.dialMinBackoff, n.dialMaxBackoff, attempt)
		n.loggerDebug("dial failed; reconnecting with backoff",
			"target", target, "attempt", attempt+1, "delay", delay, "error", err)
		select {
		case <-time.After(delay):
		case <-n.shutdownCh:
			return nil, ErrTransportShutdown
		}
	}

	// Wrap the conn
	netConn := &netConn{
		target: target,
		conn:   conn,
		dec:    codec.NewDecoder(bufio.NewReader(conn), &codec.MsgpackHandle{}),
		w:      bufio.NewWriterSize(conn, connSendBufferSize),
	}

	mp := &codec.MsgpackHandle{}
	mp.TimeNotBuiltin = !n.msgpackUseNewTimeFormat
	netConn.enc = codec.NewEncoder(netConn.w, mp)
	n.trackNetConn(netConn)

	// Done
	return netConn, nil
}

func (n *NetworkTransport) loggerDebug(msg string, args ...interface{}) {
	if n.logger != nil {
		n.logger.Debug(msg, args...)
	}
}

// dialBackoffDelay returns the delay before retry attempt. attempt is
// zero-based; the first failure uses min and each subsequent failure doubles
// it up to max.
func dialBackoffDelay(min, max time.Duration, attempt int) time.Duration {
	if min <= 0 {
		min = DefaultReconnectMinDelay
	}
	if max <= 0 {
		max = DefaultReconnectMaxDelay
	}
	delay := min
	for i := 0; i < attempt; i++ {
		delay *= 2
		if delay >= max {
			return max
		}
	}
	if delay > max {
		return max
	}
	return delay
}

// returnConn returns a connection back to the pool.
func (n *NetworkTransport) returnConn(conn *netConn) {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	key := conn.target
	conns := n.connPool[key]

	if !n.IsShutdown() && len(conns) < n.maxPool {
		n.connPool[key] = append(conns, conn)
	} else {
		_ = conn.Release()
	}
}

// AppendEntriesPipeline returns an interface that can be used to pipeline
// AppendEntries requests.
func (n *NetworkTransport) AppendEntriesPipeline(id ServerID, target ServerAddress) (AppendPipeline, error) {
	if n.maxInFlight < minInFlightForPipelining {
		// Pipelining is disabled since no more than one request can be outstanding
		// at once. Skip the whole code path and use synchronous requests.
		return nil, ErrPipelineReplicationNotSupported
	}

	// Get a connection
	conn, err := n.getConnFromAddressProvider(id, target)
	if err != nil {
		return nil, err
	}

	// Create the pipeline
	return newNetPipeline(n, conn, n.maxInFlight), nil
}

// AppendEntries implements the Transport interface.
func (n *NetworkTransport) AppendEntries(id ServerID, target ServerAddress, args *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	return n.genericRPC(id, target, rpcAppendEntries, args, resp)
}

// RequestVote implements the Transport interface.
func (n *NetworkTransport) RequestVote(id ServerID, target ServerAddress, args *RequestVoteRequest, resp *RequestVoteResponse) error {
	return n.genericRPC(id, target, rpcRequestVote, args, resp)
}

// RequestPreVote implements the Transport interface.
func (n *NetworkTransport) RequestPreVote(id ServerID, target ServerAddress, args *RequestPreVoteRequest, resp *RequestPreVoteResponse) error {
	return n.genericRPC(id, target, rpcRequestPreVote, args, resp)
}

// genericRPC handles a simple request/response RPC.
func (n *NetworkTransport) genericRPC(id ServerID, target ServerAddress, rpcType uint8, args interface{}, resp interface{}) error {
	if !n.beginRequest() {
		return ErrTransportShutdown
	}
	defer n.inflight.Done()

	// Get a conn
	conn, err := n.getConnFromAddressProvider(id, target)
	if err != nil {
		return err
	}

	corr := rpcCorrelation{Term: requestTerm(args), Sequence: n.nextSequence()}

	// Set a deadline
	if n.timeout > 0 {
		_ = conn.conn.SetDeadline(time.Now().Add(n.timeout))
	}

	// Send the RPC
	if err = sendRPC(conn, rpcType, args, corr); err != nil {
		return err
	}

	// Decode the response
	respCorr, canReturn, err := decodeCorrelatedResponse(conn, resp, corr)
	if canReturn {
		n.returnConn(conn)
	}
	if err != nil {
		if errors.Is(err, ErrStaleResponse) {
			n.loggerWarn("discarding response with stale correlation",
				"target", target, "expected", corr, "received", respCorr)
		}
		if n.IsShutdown() && !errors.Is(err, ErrStaleResponse) {
			return fmt.Errorf("%w: %v", ErrTransportShutdown, err)
		}
		return err
	}
	return nil
}

// InstallSnapshot implements the Transport interface.
func (n *NetworkTransport) InstallSnapshot(id ServerID, target ServerAddress, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.Reader) error {
	if !n.beginRequest() {
		return ErrTransportShutdown
	}
	defer n.inflight.Done()

	// Get a conn, always close for InstallSnapshot
	conn, err := n.getConnFromAddressProvider(id, target)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Release() }()

	// Set a deadline, scaled by request size
	if n.timeout > 0 {
		timeout := n.timeout * time.Duration(args.Size/int64(n.TimeoutScale))
		if timeout < n.timeout {
			timeout = n.timeout
		}
		_ = conn.conn.SetDeadline(time.Now().Add(timeout))
	}

	corr := rpcCorrelation{Term: args.Term, Sequence: n.nextSequence()}

	// Send the RPC
	if err = sendRPC(conn, rpcInstallSnapshot, args, corr); err != nil {
		return err
	}

	// Stream the state
	if _, err = io.Copy(conn.w, data); err != nil {
		return err
	}

	// Flush
	if err = conn.w.Flush(); err != nil {
		return err
	}

	// Decode the response, do not return conn
	_, _, err = decodeCorrelatedResponse(conn, resp, corr)
	return err
}

// EncodePeer implements the Transport interface.
func (n *NetworkTransport) EncodePeer(id ServerID, p ServerAddress) []byte {
	address := n.getProviderAddressOrFallback(id, p)
	return []byte(address)
}

// DecodePeer implements the Transport interface.
func (n *NetworkTransport) DecodePeer(buf []byte) ServerAddress {
	return ServerAddress(buf)
}

// TimeoutNow implements the Transport interface.
func (n *NetworkTransport) TimeoutNow(id ServerID, target ServerAddress, args *TimeoutNowRequest, resp *TimeoutNowResponse) error {
	return n.genericRPC(id, target, rpcTimeoutNow, args, resp)
}

// listen is used to handling incoming connections.
func (n *NetworkTransport) listen() {
	const baseDelay = 5 * time.Millisecond
	const maxDelay = 1 * time.Second

	var loopDelay time.Duration
	ready := false
	for {
		if !ready {
			ready = true
			if n.listenReadyCh != nil {
				close(n.listenReadyCh)
			}
		}

		// Accept incoming connections
		conn, err := n.stream.Accept()
		if err != nil {
			if loopDelay == 0 {
				loopDelay = baseDelay
			} else {
				loopDelay *= 2
			}

			if loopDelay > maxDelay {
				loopDelay = maxDelay
			}

			if !n.IsShutdown() {
				n.logger.Error("failed to accept connection", "error", err)
			}

			select {
			case <-n.shutdownCh:
				return
			case <-time.After(loopDelay):
				continue
			}
		}
		// No error, reset loop delay
		loopDelay = 0

		n.logger.Debug("accepted connection", "local-address", n.LocalAddr(), "remote-address", conn.RemoteAddr().String())

		n.trackRawConn(conn)
		// Handle the connection in dedicated routine. Resolving the context
		// here keeps a connection accepted after CloseStreams on the fresh
		// generation; constructor readiness prevents the common startup race
		// where traffic is sent before Accept is parked.
		go n.handleConn(n.getStreamContext(), conn)
	}
}

// connSession owns the read/write state for one accepted connection. Writes
// are serialized because heartbeats can be handled concurrently with ordinary
// log RPCs on a shared connection.
type connSession struct {
	r   *bufio.Reader
	w   *bufio.Writer
	dec *codec.Decoder
	enc *codec.Encoder

	writeMu sync.Mutex
}

// incomingCommand is a decoded request ready to dispatch.
type incomingCommand struct {
	corr   rpcCorrelation
	rpc    RPC
	respCh chan RPCResponse

	isHeartbeat bool
	labels      []metrics.Label

	getTypeStart time.Time
	decodeStart  time.Time
	processStart time.Time

	// finishedCh is non-nil for frames that must not be followed on the stream
	// until their handler has consumed all framed bytes (InstallSnapshot).
	finishedCh chan struct{}
}

// handleConn is used to handle an inbound connection for its lifespan. The
// handler will exit when the passed context is cancelled or the connection is
// closed.
func (n *NetworkTransport) handleConn(connCtx context.Context, conn net.Conn) {
	defer n.untrackRawConn(conn)
	defer func() { _ = conn.Close() }()

	r := bufio.NewReaderSize(conn, connReceiveBufferSize)
	w := bufio.NewWriter(conn)
	dec := codec.NewDecoder(r, &codec.MsgpackHandle{})

	mp := &codec.MsgpackHandle{}
	mp.TimeNotBuiltin = !n.msgpackUseNewTimeFormat
	enc := codec.NewEncoder(w, mp)
	sess := &connSession{r: r, w: w, dec: dec, enc: enc}

	// Ordinary RPCs are processed by one worker in stream order. Heartbeat
	// fast-paths bypass this queue so a slow log append or disk write cannot
	// prevent a heartbeat sharing the same connection from being answered.
	normalCh := make(chan *incomingCommand, 256)
	normalDone := make(chan struct{})
	go func() {
		defer close(normalDone)
		for cmd := range normalCh {
			n.processCommand(sess, cmd)
		}
	}()
	defer close(normalCh)

	for {
		select {
		case <-connCtx.Done():
			n.loggerDebug("stream layer is closed")
			return
		default:
		}

		cmd, err := n.readCommand(sess)
		if err != nil {
			if err != io.EOF && !errors.Is(err, net.ErrClosed) && !n.IsShutdown() {
				n.logger.Error("failed to decode incoming command", "error", err)
			}
			return
		}

		// CloseStreams cancels this generation's context between frames. Stop
		// before accepting more work even if bytes are already buffered.
		select {
		case <-connCtx.Done():
			return
		default:
		}

		// Close's WaitGroup must account for every decoded command before the
		// shutdown flag is observed.
		if !n.beginRequest() {
			n.finishCommand(cmd)
			return
		}

		// Heartbeat fast-path work is independent of the ordered normal-RPC
		// worker. If no handler is installed it must go through the consumer.
		if cmd.isHeartbeat && n.heartbeatHandler() != nil {
			go n.processCommand(sess, cmd)
			continue
		}

		select {
		case normalCh <- cmd:
		case <-n.shutdownCh:
			n.inflight.Done()
			n.finishCommand(cmd)
			return
		case <-connCtx.Done():
			n.inflight.Done()
			n.finishCommand(cmd)
			return
		}

		// InstallSnapshot's body continues on sess.r. Do not read the next
		// frame until the worker has consumed exactly req.Size bytes.
		if cmd.finishedCh != nil {
			select {
			case <-cmd.finishedCh:
			case <-n.shutdownCh:
				return
			case <-connCtx.Done():
				return
			}
		}
	}
}

func (n *NetworkTransport) heartbeatHandler() func(RPC) {
	n.heartbeatFnLock.Lock()
	defer n.heartbeatFnLock.Unlock()
	return n.heartbeatFn
}

func (n *NetworkTransport) finishCommand(cmd *incomingCommand) {
	if cmd != nil && cmd.finishedCh != nil {
		select {
		case <-cmd.finishedCh:
		default:
			close(cmd.finishedCh)
		}
	}
}

// readCommand is used to decode a single command and its correlation header.
func (n *NetworkTransport) readCommand(sess *connSession) (*incomingCommand, error) {
	getTypeStart := time.Now()

	// Get the rpc type
	rpcType, err := sess.r.ReadByte()
	if err != nil {
		return nil, err
	}

	// measuring the time to get the first byte separately because the heartbeat conn will hang out here
	// for a good while waiting for a heartbeat whereas the append entries/rpc conn should not.
	metrics.MeasureSince([]string{"raft", "net", "getRPCType"}, getTypeStart)
	decodeStart := time.Now()

	var corr rpcCorrelation
	if err := sess.dec.Decode(&corr); err != nil {
		return nil, err
	}

	cmd := &incomingCommand{
		corr:         corr,
		getTypeStart: getTypeStart,
		decodeStart:  decodeStart,
	}
	respCh := make(chan RPCResponse, 1)
	cmd.respCh = respCh
	cmd.rpc = RPC{RespChan: respCh}

	// Decode the command
	isHeartbeat := false
	var labels []metrics.Label
	switch rpcType {
	case rpcAppendEntries:
		var req AppendEntriesRequest
		if err := sess.dec.Decode(&req); err != nil {
			return nil, err
		}
		cmd.rpc.Command = &req

		leaderAddr := req.Addr
		if len(leaderAddr) == 0 {
			leaderAddr = req.Leader
		}

		// Check if this is a heartbeat
		if req.Term != 0 && leaderAddr != nil &&
			req.PrevLogEntry == 0 && req.PrevLogTerm == 0 &&
			len(req.Entries) == 0 && req.LeaderCommitIndex == 0 {
			isHeartbeat = true
		}

		if isHeartbeat {
			labels = []metrics.Label{{Name: "rpcType", Value: "Heartbeat"}}
		} else {
			labels = []metrics.Label{{Name: "rpcType", Value: "AppendEntries"}}
		}
	case rpcRequestVote:
		var req RequestVoteRequest
		if err := sess.dec.Decode(&req); err != nil {
			return nil, err
		}
		cmd.rpc.Command = &req
		labels = []metrics.Label{{Name: "rpcType", Value: "RequestVote"}}
	case rpcRequestPreVote:
		var req RequestPreVoteRequest
		if err := sess.dec.Decode(&req); err != nil {
			return nil, err
		}
		cmd.rpc.Command = &req
		labels = []metrics.Label{{Name: "rpcType", Value: "RequestPreVote"}}
	case rpcInstallSnapshot:
		var req InstallSnapshotRequest
		if err := sess.dec.Decode(&req); err != nil {
			return nil, err
		}
		cmd.rpc.Command = &req
		cmd.rpc.Reader = io.LimitReader(sess.r, req.Size)
		cmd.finishedCh = make(chan struct{})
		labels = []metrics.Label{{Name: "rpcType", Value: "InstallSnapshot"}}
	case rpcTimeoutNow:
		var req TimeoutNowRequest
		if err := sess.dec.Decode(&req); err != nil {
			return nil, err
		}
		cmd.rpc.Command = &req
		labels = []metrics.Label{{Name: "rpcType", Value: "TimeoutNow"}}
	default:
		return nil, fmt.Errorf("unknown rpc type %d", rpcType)
	}

	cmd.isHeartbeat = isHeartbeat
	cmd.labels = labels
	metrics.MeasureSinceWithLabels([]string{"raft", "net", "rpcDecode"}, decodeStart, labels)
	cmd.processStart = time.Now()
	return cmd, nil
}

// processCommand dispatches one decoded command if necessary and writes its
// correlated response. It always releases the transport inflight ticket.
func (n *NetworkTransport) processCommand(sess *connSession, cmd *incomingCommand) {
	defer n.inflight.Done()
	if cmd.finishedCh != nil {
		defer close(cmd.finishedCh)
	}

	// Check for heartbeat fast-path
	if cmd.isHeartbeat {
		if fn := n.heartbeatHandler(); fn != nil {
			fn(cmd.rpc)
			n.writeCommandResponse(sess, cmd)
			return
		}
	}

	// Dispatch the RPC
	select {
	case n.consumeCh <- cmd.rpc:
	case <-n.shutdownCh:
		return
	}

	n.writeCommandResponse(sess, cmd)
}

func (n *NetworkTransport) writeCommandResponse(sess *connSession, cmd *incomingCommand) {
	// we will differentiate the heartbeat fast path from normal RPCs with labels
	metrics.MeasureSinceWithLabels([]string{"raft", "net", "rpcEnqueue"}, cmd.processStart, cmd.labels)
	respWaitStart := time.Now()

	var resp RPCResponse
	select {
	case resp = <-cmd.respCh:
	case <-n.forceCloseCh:
		return
	}
	metrics.MeasureSinceWithLabels([]string{"raft", "net", "rpcRespond"}, respWaitStart, cmd.labels)

	// Encode atomically with other responses sharing this connection.
	sess.writeMu.Lock()
	defer sess.writeMu.Unlock()

	if err := sess.enc.Encode(cmd.corr); err != nil {
		n.loggerDebug("failed to encode response correlation", "error", err)
		return
	}

	// Send the error first
	respErr := ""
	if resp.Error != nil {
		respErr = resp.Error.Error()
	}
	if err := sess.enc.Encode(respErr); err != nil {
		n.loggerDebug("failed to encode RPC error response", "error", err)
		return
	}

	// Send the response
	if err := sess.enc.Encode(resp.Response); err != nil {
		n.loggerDebug("failed to encode RPC response", "error", err)
		return
	}
	if err := sess.w.Flush(); err != nil {
		n.loggerDebug("failed to flush response", "error", err)
	}
}

// decodeResponse preserves the historical response format: an error string
// followed by the response value. It reports whether the connection can be
// reused. New correlated RPCs use decodeCorrelatedResponse.
func decodeResponse(conn *netConn, resp interface{}) (bool, error) {
	rpcError, err := decodeResponseFrame(conn, resp)
	if err != nil {
		return false, err
	}
	if rpcError != "" {
		return true, errors.New(rpcError)
	}
	return true, nil
}

// decodeCorrelatedResponse decodes the response correlation header followed by
// the historical error string and response value.
func decodeCorrelatedResponse(conn *netConn, resp interface{}, want rpcCorrelation) (rpcCorrelation, bool, error) {
	var got rpcCorrelation
	if err := conn.dec.Decode(&got); err != nil {
		_ = conn.Release()
		return got, false, err
	}
	rpcError, err := decodeResponseFrame(conn, resp)
	if err != nil {
		return got, false, err
	}

	// A nonzero expected sequence identifies the request. Reject any mismatch
	// before Raft can consult the response payload. The whole frame has now
	// been consumed, but the connection is retired because a mismatch means
	// its correlation state is not trustworthy.
	if want.Sequence != 0 && (got.Sequence != want.Sequence || got.Term != want.Term) {
		_ = conn.Release()
		return got, false, fmt.Errorf("%w: expected term=%d seq=%d, got term=%d seq=%d",
			ErrStaleResponse, want.Term, want.Sequence, got.Term, got.Sequence)
	}

	// Format an error if any
	if rpcError != "" {
		return got, true, errors.New(rpcError)
	}
	return got, true, nil
}

// decodeResponseFrame decodes the legacy error string and response payload.
// It releases the connection on decode failures because the stream can no
// longer be kept in sync.
func decodeResponseFrame(conn *netConn, resp interface{}) (string, error) {
	var rpcError string
	if err := conn.dec.Decode(&rpcError); err != nil {
		_ = conn.Release()
		return "", err
	}
	if err := conn.dec.Decode(resp); err != nil {
		_ = conn.Release()
		return rpcError, err
	}
	return rpcError, nil
}

// sendRPC is used to encode and send the RPC. The optional correlation
// argument preserves source compatibility with the historical three-argument
// call form; production RPCs pass exactly one correlation header.
func sendRPC(conn *netConn, rpcType uint8, args interface{}, correlation ...rpcCorrelation) error {
	conn.writeMu.Lock()
	defer conn.writeMu.Unlock()

	// Write the request type
	if err := conn.w.WriteByte(rpcType); err != nil {
		_ = conn.Release()
		return err
	}

	// Write the optional correlation metadata.
	if len(correlation) > 0 {
		if err := conn.enc.Encode(correlation[0]); err != nil {
			_ = conn.Release()
			return err
		}
	}

	// Send the request
	if err := conn.enc.Encode(args); err != nil {
		_ = conn.Release()
		return err
	}

	// Flush
	if err := conn.w.Flush(); err != nil {
		_ = conn.Release()
		return err
	}
	return nil
}

// newNetPipeline is used to construct a netPipeline from a given transport and
// connection. It is a bug to ever call this with maxInFlight less than 2
// (minInFlightForPipelining) and will cause a panic.
func newNetPipeline(trans *NetworkTransport, conn *netConn, maxInFlight int) *netPipeline {
	if maxInFlight < minInFlightForPipelining {
		// Shouldn't happen (tm) since we validate this in the one call site and
		// skip pipelining if it's lower.
		panic("pipelining makes no sense if maxInFlight < 2")
	}
	n := &netPipeline{
		conn:  conn,
		trans: trans,
		// The historical handoff channel is retained for tests and tooling
		// that inspect pipeline state. Response correlation now uses pending.
		inprogressCh: make(chan *appendFuture, maxInFlight-2),
		doneCh:       make(chan AppendFuture, maxInFlight-2),
		pending:      make(map[uint64]*pendingAppend),
		// The historical effective limit is maxInFlight-1 accepted calls: one
		// additional call may have written its request and is blocked applying
		// back-pressure. Capacity maxInFlight-1 reproduces that contract.
		slots:      make(chan struct{}, maxInFlight-1),
		shutdownCh: make(chan struct{}),
	}
	go n.decodeResponses()
	return n
}

// addPending stores a future under its sequence and adds its WaitGroup ticket
// atomically with the shutdown check. It returns false if the pipeline has
// started closing.
func (p *netPipeline) addPending(seq uint64, future *appendFuture, corr rpcCorrelation) bool {
	p.shutdownLock.Lock()
	defer p.shutdownLock.Unlock()
	if p.shutdown {
		return false
	}
	p.pendingWG.Add(1)
	p.pendingMu.Lock()
	p.pending[seq] = &pendingAppend{future: future, corr: corr}
	p.pendingMu.Unlock()
	return true
}

// takeFuture removes and returns the future for seq. A second response for the
// same sequence, or a frame from an old generation, returns nil and is
// discarded by the caller.
func (p *netPipeline) takePending(seq uint64) *pendingAppend {
	p.pendingMu.Lock()
	defer p.pendingMu.Unlock()
	entry := p.pending[seq]
	delete(p.pending, seq)
	return entry
}

func (p *netPipeline) failAllPending(err error) {
	p.pendingMu.Lock()
	pending := p.pending
	p.pending = make(map[uint64]*pendingAppend)
	p.pendingMu.Unlock()

	for _, entry := range pending {
		future := entry.future
		future.respond(err)
		p.pendingWG.Done()
		p.trans.inflight.Done()
		select {
		case p.doneCh <- future:
		default:
		}
	}
}

// decodeResponses is a long running routine that decodes the responses sent
// on the connection and matches them to futures by sequence number. It is
// therefore safe for response frames to become available in a different order
// than the requests were sent.
func (n *netPipeline) decodeResponses() {
	timeout := n.trans.timeout
	for {
		if timeout > 0 {
			_ = n.conn.conn.SetReadDeadline(time.Now().Add(timeout))
		}

		var got rpcCorrelation
		if err := n.conn.dec.Decode(&got); err != nil {
			err = n.closeError(err)
			n.failAllPending(err)
			n.markShutdown()
			return
		}

		entry := n.takePending(got.Sequence)
		if entry == nil {
			// No waiter: this is either an expired duplicate or a frame from
			// an earlier pipeline generation. Consume and discard its body
			// without touching Raft state.
			var discarded interface{}
			if _, err := decodeResponseFrame(n.conn, &discarded); err != nil {
				n.failAllPending(n.closeError(err))
				n.markShutdown()
				return
			}
			n.trans.loggerDebug("discarded unmatched pipeline response", "correlation", got)
			continue
		}
		future := entry.future

		rpcError, err := decodeResponseFrame(n.conn, future.resp)
		if err == nil && (got.Term != entry.corr.Term || got.Sequence != entry.corr.Sequence) {
			err = fmt.Errorf("%w: expected term=%d seq=%d, got term=%d seq=%d",
				ErrStaleResponse, entry.corr.Term, entry.corr.Sequence, got.Term, got.Sequence)
		}
		if err == nil && rpcError != "" {
			err = errors.New(rpcError)
		}

		future.respond(err)

		// Delivery is part of normal completion. Block for the consumer, but
		// always release accounting if Close stops the pipeline/transport.
		select {
		case n.doneCh <- future:
		case <-n.shutdownCh:
			<-n.slots
			n.pendingWG.Done()
			n.trans.inflight.Done()
			return
		case <-n.trans.shutdownCh:
			<-n.slots
			n.pendingWG.Done()
			n.trans.inflight.Done()
			return
		}

		<-n.slots
		n.pendingWG.Done()
		n.trans.inflight.Done()
	}
}

func (n *netPipeline) closeError(err error) error {
	if n.isShutdown() || n.trans.IsShutdown() {
		return ErrPipelineShutdown
	}
	return err
}

func (n *netPipeline) isShutdown() bool {
	n.shutdownLock.Lock()
	defer n.shutdownLock.Unlock()
	return n.shutdown
}

// markShutdown closes shutdownCh at most once. It does not close the network
// connection; callers that need to unblock I/O should Release it separately.
func (n *netPipeline) markShutdown() {
	n.shutdownLock.Lock()
	defer n.shutdownLock.Unlock()
	if !n.shutdown {
		n.shutdown = true
		close(n.shutdownCh)
	}
}

// AppendEntries is used to pipeline a new append entries request.
func (n *netPipeline) AppendEntries(args *AppendEntriesRequest, resp *AppendEntriesResponse) (AppendFuture, error) {
	// Bound the number of unfinished requests. This is the transport-level
	// back-pressure and works independently of response arrival order.
	select {
	case n.slots <- struct{}{}:
	case <-n.shutdownCh:
		return nil, ErrPipelineShutdown
	}
	if !n.trans.beginRequest() {
		<-n.slots
		return nil, ErrTransportShutdown
	}

	// Create a new future
	corr := rpcCorrelation{Term: args.Term, Sequence: n.trans.nextSequence()}
	future := &appendFuture{
		start: time.Now(),
		args:  args,
		resp:  resp,
	}
	future.init()

	if !n.addPending(corr.Sequence, future, corr) {
		n.trans.inflight.Done()
		<-n.slots
		return nil, ErrPipelineShutdown
	}

	// Add a send timeout and send atomically.
	if timeout := n.trans.timeout; timeout > 0 {
		_ = n.conn.conn.SetWriteDeadline(time.Now().Add(timeout))
	}
	if err := sendRPC(n.conn, rpcAppendEntries, future.args, corr); err != nil {
		if removed := n.takePending(corr.Sequence); removed != nil {
			removed.future.respond(err)
			n.pendingWG.Done()
			n.trans.inflight.Done()
		}
		<-n.slots
		n.failAllPending(err)
		n.markShutdown()
		return nil, err
	}

	// The decoder matches the response by corr.Sequence. Retain a non-blocking
	// hand-off to the historical channel for observability/compatibility, but
	// never block response decoding on it.
	select {
	case n.inprogressCh <- future:
	default:
	}

	return future, nil
}

// Consumer returns a channel that can be used to consume complete futures.
func (n *netPipeline) Consumer() <-chan AppendFuture {
	return n.doneCh
}

// Close stops new requests and waits for already accepted requests to finish
// naturally up to the transport's CloseTimeout. If the timeout expires it
// forcibly releases the connection, completing every remaining future with
// ErrPipelineShutdown.
func (n *netPipeline) Close() error {
	n.shutdownLock.Lock()
	if n.shutdown {
		n.shutdownLock.Unlock()
		return nil
	}
	n.shutdown = true
	close(n.shutdownCh)
	closeTimeout := n.trans.gracefulCloseTimeout()
	n.shutdownLock.Unlock()

	done := make(chan struct{})
	go func() {
		n.pendingWG.Wait()
		close(done)
	}()

	timer := time.NewTimer(closeTimeout)
	defer timer.Stop()

	select {
	case <-done:
		// All requests completed normally.
	case <-timer.C:
		// Force the decoder/read and any blocked writer to unblock. Releasing
		// first lets failAllPending own terminal completion uniformly.
		_ = n.conn.Release()
		n.failAllPending(ErrPipelineShutdown)
	}

	// Release is idempotent and stops the decoder's next blocking read once
	// all pending work has drained naturally.
	_ = n.conn.Release()
	return nil
}

func (n *NetworkTransport) handleCommand(r *bufio.Reader, dec *codec.Decoder, enc *codec.Encoder) error {
	getTypeStart := time.Now()

	// Get the rpc type
	rpcType, err := r.ReadByte()
	if err != nil {
		return err
	}

	// measuring the time to get the first byte separately because the heartbeat conn will hang out here
	// for a good while waiting for a heartbeat whereas the append entries/rpc conn should not.
	metrics.MeasureSince([]string{"raft", "net", "getRPCType"}, getTypeStart)
	decodeStart := time.Now()

	// Create the RPC object
	respCh := make(chan RPCResponse, 1)
	rpc := RPC{
		RespChan: respCh,
	}

	// Decode the command
	isHeartbeat := false
	var labels []metrics.Label
	switch rpcType {
	case rpcAppendEntries:
		var req AppendEntriesRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req

		leaderAddr := req.Addr
		if len(leaderAddr) == 0 {
			leaderAddr = req.Leader
		}

		// Check if this is a heartbeat
		if req.Term != 0 && leaderAddr != nil &&
			req.PrevLogEntry == 0 && req.PrevLogTerm == 0 &&
			len(req.Entries) == 0 && req.LeaderCommitIndex == 0 {
			isHeartbeat = true
		}

		if isHeartbeat {
			labels = []metrics.Label{{Name: "rpcType", Value: "Heartbeat"}}
		} else {
			labels = []metrics.Label{{Name: "rpcType", Value: "AppendEntries"}}
		}
	case rpcRequestVote:
		var req RequestVoteRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		labels = []metrics.Label{{Name: "rpcType", Value: "RequestVote"}}
	case rpcRequestPreVote:
		var req RequestPreVoteRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		labels = []metrics.Label{{Name: "rpcType", Value: "RequestPreVote"}}
	case rpcInstallSnapshot:
		var req InstallSnapshotRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		rpc.Reader = io.LimitReader(r, req.Size)
		labels = []metrics.Label{{Name: "rpcType", Value: "InstallSnapshot"}}
	case rpcTimeoutNow:
		var req TimeoutNowRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		labels = []metrics.Label{{Name: "rpcType", Value: "TimeoutNow"}}
	default:
		return fmt.Errorf("unknown rpc type %d", rpcType)
	}

	metrics.MeasureSinceWithLabels([]string{"raft", "net", "rpcDecode"}, decodeStart, labels)

	processStart := time.Now()

	// Check for heartbeat fast-path
	if isHeartbeat {
		n.heartbeatFnLock.Lock()
		fn := n.heartbeatFn
		n.heartbeatFnLock.Unlock()
		if fn != nil {
			fn(rpc)
			goto RESP
		}
	}

	// Dispatch the RPC
	select {
	case n.consumeCh <- rpc:
	case <-n.shutdownCh:
		return ErrTransportShutdown
	}

	// Wait for response
RESP:
	// we will differentiate the heartbeat fast path from normal RPCs with labels
	metrics.MeasureSinceWithLabels([]string{"raft", "net", "rpcEnqueue"}, processStart, labels)
	respWaitStart := time.Now()
	select {
	case resp := <-respCh:
		defer metrics.MeasureSinceWithLabels([]string{"raft", "net", "rpcRespond"}, respWaitStart, labels)
		// Send the error first
		respErr := ""
		if resp.Error != nil {
			respErr = resp.Error.Error()
		}
		if err := enc.Encode(respErr); err != nil {
			return err
		}

		// Send the response
		if err := enc.Encode(resp.Response); err != nil {
			return err
		}
	case <-n.shutdownCh:
		return ErrTransportShutdown
	}
	return nil
}
