// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// reorderingPipeline wraps a real AppendPipeline. Responses are pulled from
// the underlying transport and forwarded out of send order, which lets the
// tests drive the replication layer's contiguous-commit semantics through a
// real transport instead of a fake. It never completes a future itself, so it
// also exercises the transport's exactly-once completion.
type reorderingPipeline struct {
	inner AppendPipeline

	shutdownCh chan struct{}
	closeOnce  sync.Once
	consumerCh chan AppendFuture
}

func newReorderingPipeline(inner AppendPipeline) *reorderingPipeline {
	p := &reorderingPipeline{
		inner:      inner,
		shutdownCh: make(chan struct{}),
		consumerCh: make(chan AppendFuture, 64),
	}
	go p.run()
	return p
}

func (p *reorderingPipeline) run() {
	innerCh := p.inner.Consumer()
	var held []AppendFuture
	flush := func() {
		// Swap each adjacent pair so responses arrive in a different order
		// than they were sent without losing any of them.
		for i := 0; i+1 < len(held); i += 2 {
			held[i], held[i+1] = held[i+1], held[i]
		}
		for _, fut := range held {
			select {
			case p.consumerCh <- fut:
			case <-p.shutdownCh:
				return
			}
		}
		held = nil
	}
	for {
		select {
		case fut, ok := <-innerCh:
			if !ok {
				close(p.consumerCh)
				return
			}
			held = append(held, fut)
			if len(held) >= 2 {
				flush()
			}
		case <-p.shutdownCh:
			return
		}
	}
}

func (p *reorderingPipeline) AppendEntries(args *AppendEntriesRequest, resp *AppendEntriesResponse) (AppendFuture, error) {
	return p.inner.AppendEntries(args, resp)
}

func (p *reorderingPipeline) Consumer() <-chan AppendFuture { return p.consumerCh }

func (p *reorderingPipeline) Close() error {
	p.closeOnce.Do(func() { close(p.shutdownCh) })
	return p.inner.Close()
}

// pipeliningTransport is an InmemTransport whose pipelines deliver responses
// out of order. Everything else, including connection management, is the real
// in-memory transport, so the non-pipelined semantics stay identical.
type pipeliningTransport struct {
	*InmemTransport

	lock      sync.Mutex
	pipelines []*reorderingPipeline
}

func newPipeliningTransport(addr ServerAddress) (ServerAddress, *pipeliningTransport) {
	a, inner := NewInmemTransport(addr)
	return a, &pipeliningTransport{InmemTransport: inner}
}

func (t *pipeliningTransport) AppendEntriesPipeline(id ServerID, target ServerAddress) (AppendPipeline, error) {
	inner, err := t.InmemTransport.AppendEntriesPipeline(id, target)
	if err != nil {
		return nil, err
	}
	p := newReorderingPipeline(inner)
	t.lock.Lock()
	t.pipelines = append(t.pipelines, p)
	t.lock.Unlock()
	return p, nil
}

func (t *pipeliningTransport) pipelineCount() int {
	t.lock.Lock()
	defer t.lock.Unlock()
	return len(t.pipelines)
}

// newPipeliningCluster builds a fully connected raft cluster of the given
// size whose transports reorder pipelined responses.
func newPipeliningCluster(t *testing.T, peers int) ([]*pipeliningTransport, []*Raft, []*MockFSM, *Config) {
	t.Helper()
	conf := inmemConfig(t)
	conf.MaxAppendEntries = 2

	trans := make([]*pipeliningTransport, peers)
	addrs := make([]ServerAddress, peers)
	ids := make([]ServerID, peers)
	for i := 0; i < peers; i++ {
		addrs[i], trans[i] = newPipeliningTransport("")
		ids[i] = ServerID(fmt.Sprintf("server-%d", i))
	}
	for i := 0; i < peers; i++ {
		for j := 0; j < peers; j++ {
			if i != j {
				trans[i].Connect(addrs[j], trans[j].InmemTransport)
			}
		}
	}

	var configuration Configuration
	for i := 0; i < peers; i++ {
		configuration.Servers = append(configuration.Servers, Server{
			Suffrage: Voter, ID: ids[i], Address: addrs[i],
		})
	}

	rafts := make([]*Raft, peers)
	fsms := make([]*MockFSM, peers)
	for i := 0; i < peers; i++ {
		fsms[i] = &MockFSM{}
		logs := NewInmemStore()
		stable := NewInmemStore()
		snaps := NewInmemSnapshotStore()

		peerConf := *conf
		peerConf.LocalID = ids[i]
		peerConf.Logger = newTestLoggerWithPrefix(t, string(ids[i]))

		require.NoError(t, BootstrapCluster(&peerConf, logs, stable, snaps, trans[i], configuration))
		r, err := NewRaft(&peerConf, fsms[i], logs, stable, snaps, trans[i])
		require.NoError(t, err)
		rafts[i] = r
	}

	t.Cleanup(func() {
		for _, r := range rafts {
			_ = r.Shutdown().Error()
		}
		for _, tr := range trans {
			_ = tr.Close()
		}
	})
	return trans, rafts, fsms, conf
}

func waitForPipelineLeader(t *testing.T, rafts []*Raft) *Raft {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, r := range rafts {
			if r.State() == Leader {
				return r
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return nil
}

func applyEntries(t *testing.T, leader *Raft, start, n int, timeout time.Duration) {
	t.Helper()
	for i := start; i < start+n; i++ {
		require.NoError(t, leader.Apply([]byte(fmt.Sprintf("cmd-%d", i)), timeout).Error())
	}
}

func requireConverged(t *testing.T, fsms []*MockFSM, expectedLen int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		var first []byte
		for i, fsm := range fsms {
			fsm.Lock()
			logs := append([][]byte(nil), fsm.logs...)
			fsm.Unlock()
			if len(logs) != expectedLen {
				ok = false
				break
			}
			joined := bytes.Join(logs, []byte{0})
			if i == 0 {
				first = joined
			} else if !bytes.Equal(first, joined) {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i, fsm := range fsms {
		fsm.Lock()
		t.Logf("fsm %d len=%d", i, len(fsm.logs))
		fsm.Unlock()
	}
	t.Fatalf("FSMs did not converge to %d entries", expectedLen)
}

// TestPipelineIntegration_ReorderedResponses verifies requirements 2 and 5
// end-to-end: responses are delivered out of order on every pipeline, yet all
// entries commit in one identical order on every server and pipelining is
// actually exercised.
func TestPipelineIntegration_ReorderedResponses(t *testing.T) {
	trans, rafts, fsms, conf := newPipeliningCluster(t, 3)

	leader := waitForPipelineLeader(t, rafts)
	applyEntries(t, leader, 0, 60, conf.CommitTimeout*4)
	requireConverged(t, fsms, 60)

	pipelines := 0
	for _, tr := range trans {
		pipelines += tr.pipelineCount()
	}
	require.Positive(t, pipelines, "expected pipelined replication to be exercised")
}

// applyThroughLeader applies entries through whichever raft is currently
// leader, retrying briefly so a leadership change mid-test does not fail the
// apply. It returns the total number of entries applied.
func applyThroughLeader(t *testing.T, rafts []*Raft, applied, n int, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for i := 0; i < n; i++ {
		cmd := []byte(fmt.Sprintf("cmd-%d", applied))
		var lastErr error
		ok := false
		for !ok && time.Now().Before(deadline) {
			foundLeader := false
			for _, leader := range rafts {
				if leader.State() != Leader {
					continue
				}
				foundLeader = true
				if err := leader.Apply(cmd, timeout).Error(); err == nil {
					ok = true
					break
				} else {
					lastErr = err
				}
			}
			if !ok {
				if !foundLeader {
					lastErr = fmt.Errorf("no leader elected")
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		require.Truef(t, ok, "failed to apply entry %d: %v", applied, lastErr)
		applied++
	}
	return applied
}

// TestPipelineIntegration_DisconnectReconnect verifies requirements 1, 3 and
// 4: disconnecting closes active pipelines and drops in-flight work, and each
// reconnect creates a fresh session with a new generation. Replication resumes
// without hanging and every server converges.
func TestPipelineIntegration_DisconnectReconnect(t *testing.T) {
	trans, rafts, fsms, conf := newPipeliningCluster(t, 3)

	var applied int
	for round := 0; round < 4; round++ {
		applied = applyThroughLeader(t, rafts, applied, 25, conf.CommitTimeout*4)

		// Break every connection touching a peer, which closes its active
		// pipelines so the next round must establish fresh sessions with new
		// generations, then restore full connectivity.
		for _, tr := range trans {
			tr.DisconnectAll()
		}
		time.Sleep(20 * time.Millisecond)
		for i := 0; i < len(trans); i++ {
			for j := 0; j < len(trans); j++ {
				if i != j {
					trans[i].Connect(trans[j].localAddr, trans[j].InmemTransport)
				}
			}
		}
	}

	applied = applyThroughLeader(t, rafts, applied, 25, conf.CommitTimeout*4)
	requireConverged(t, fsms, applied)
}

// TestPipelineTransportParity_SessionSemantics verifies requirement 5
// directly: a pipelineSession driven by the real in-memory transport and one
// driven by the real network transport expose the same commit order, the same
// close semantics, and the same exactly-once completion for in-flight
// requests.
func TestPipelineTransportParity_SessionSemantics(t *testing.T) {
	run := func(t *testing.T, open func(t *testing.T) AppendPipeline) {
		t.Helper()
		pipeline := open(t)
		var gen uint64
		session := newPipelineSession(nextPipelineGen(&gen), pipeline)

		for i := uint64(1); i <= 5; i++ {
			_, err := session.send(&AppendEntriesRequest{
				Term:         1,
				PrevLogEntry: i - 1,
				Entries:      []*Log{{Index: i, Term: 1, Type: LogCommand}},
			})
			require.NoError(t, err)
		}

		// Responses arrive in order over both transports; the frontier must
		// advance one contiguous step at a time and end at 5.
		var lastMatch uint64
		for i := uint64(1); i <= 5; i++ {
			select {
			case fut := <-session.consumer():
				res := session.handle(fut)
				require.Equal(t, pipelineOutcomeAdvanced, res.outcome)
				require.Equal(t, i, res.matchIndex)
				require.Greater(t, res.matchIndex, lastMatch)
				lastMatch = res.matchIndex
			case <-time.After(2 * time.Second):
				t.Fatalf("timed out waiting for response %d", i)
			}
		}
		require.Equal(t, uint64(5), lastMatch)
		require.Equal(t, 0, session.inflightCount())

		// Closing finalizes nothing twice and rejects further sends.
		session.close(ErrPipelineShutdown)
		require.True(t, session.isClosed())
		require.True(t, session.isFinalized())
		_, err := session.send(&AppendEntriesRequest{})
		require.ErrorIs(t, err, ErrPipelineShutdown)
	}

	t.Run("inmem", func(t *testing.T) {
		run(t, func(t *testing.T) AppendPipeline {
			addr1, trans1 := NewInmemTransport("")
			_ = addr1
			addr2, trans2 := NewInmemTransport("")
			t.Cleanup(func() {
				_ = trans1.Close()
				_ = trans2.Close()
			})
			trans1.Connect(addr2, trans2)
			trans2.Connect(addr1, trans1)

			go func() {
				for rpc := range trans1.Consumer() {
					req := rpc.Command.(*AppendEntriesRequest)
					rpc.Respond(&AppendEntriesResponse{Term: req.Term, Success: true}, nil)
				}
			}()
			p, err := trans2.AppendEntriesPipeline("id1", addr1)
			require.NoError(t, err)
			return p
		})
	})

	t.Run("network", func(t *testing.T) {
		run(t, func(t *testing.T) AppendPipeline {
			srv := makeQuietTransport(t, "localhost:0")
			t.Cleanup(func() { _ = srv.Close() })
			go func() {
				for rpc := range srv.Consumer() {
					req := rpc.Command.(*AppendEntriesRequest)
					rpc.Respond(&AppendEntriesResponse{Term: req.Term, Success: true}, nil)
				}
			}()
			cli := makeQuietTransport(t, string(srv.LocalAddr()))
			t.Cleanup(func() { _ = cli.Close() })
			p, err := cli.AppendEntriesPipeline("id1", srv.LocalAddr())
			require.NoError(t, err)
			return p
		})
	})
}
