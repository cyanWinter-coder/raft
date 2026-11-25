// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUID559_WindowCapBlocksAndReleases(t *testing.T) {
	s := &followerReplication{}
	s.initReplicationWindow(4)
	require.Equal(t, uint64(4), s.maxWindow())

	require.True(t, s.acquireInflight(3, nil))
	require.Equal(t, uint64(3), s.inflightCount())

	acquired := make(chan bool, 1)
	go func() {
		acquired <- s.acquireInflight(3, nil)
	}()

	select {
	case got := <-acquired:
		t.Fatalf("acquire beyond window returned early: %v", got)
	case <-time.After(20 * time.Millisecond):
	}

	// Releasing one slot leaves two outstanding, so the waiter's three-log
	// reservation still does not fit.
	s.releaseInflight(1)
	select {
	case got := <-acquired:
		t.Fatalf("acquire returned with only one released slot: %v", got)
	case <-time.After(20 * time.Millisecond):
	}

	// One more release gives the waiter exactly enough capacity.
	s.releaseInflight(1)
	select {
	case got := <-acquired:
		require.True(t, got)
	case <-time.After(time.Second):
		t.Fatal("blocked acquire was not woken by release")
	}
	require.Equal(t, uint64(4), s.inflightCount())

	s.releaseInflight(4)
	require.Eventually(t, func() bool {
		return s.inflightCount() == 0
	}, time.Second, time.Millisecond)
}

func TestUID559_WindowAbortInterruptsWait(t *testing.T) {
	s := &followerReplication{}
	s.initReplicationWindow(2)
	require.True(t, s.acquireInflight(2, nil))

	abort := make(chan struct{})
	done := make(chan bool, 1)
	go func() {
		done <- s.acquireInflight(1, abort)
	}()

	select {
	case <-done:
		t.Fatal("acquire unexpectedly completed while window was full")
	case <-time.After(20 * time.Millisecond):
	}

	close(abort)
	select {
	case ok := <-done:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("abort did not interrupt window wait")
	}

	// The interrupted waiter must not have consumed capacity.
	require.Equal(t, uint64(2), s.inflightCount())
	s.resetInflight()
	require.Equal(t, uint64(0), s.inflightCount())
	require.True(t, s.acquireInflight(2, nil))
}

func TestUID559_WindowPreclosedAbortInterruptsWait(t *testing.T) {
	s := &followerReplication{}
	s.initReplicationWindow(1)
	require.True(t, s.acquireInflight(1, nil))

	abort := make(chan struct{})
	close(abort)
	done := make(chan bool, 1)
	go func() {
		done <- s.acquireInflight(1, abort)
	}()

	select {
	case ok := <-done:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("already-closed abort did not interrupt window wait")
	}
	require.Equal(t, uint64(1), s.inflightCount())
}

func TestUID559_WindowResetReleasesAllSlots(t *testing.T) {
	s := &followerReplication{}
	s.initReplicationWindow(3)
	require.True(t, s.acquireInflight(3, nil))

	abort := make(chan struct{})
	done := make(chan bool, 1)
	go func() {
		done <- s.acquireInflight(1, abort)
	}()

	time.Sleep(20 * time.Millisecond)
	s.resetInflight()

	select {
	case ok := <-done:
		require.True(t, ok, "reset should wake waiters and allow reservation")
	case <-time.After(time.Second):
		t.Fatal("reset did not wake window waiter")
	}
	require.Equal(t, uint64(1), s.inflightCount())
}

func TestUID559_UpdateProgressIsMonotonicAndIdempotent(t *testing.T) {
	peerID := ServerID("uid559-peer")
	c := newCommitment(make(chan struct{}, 1), Configuration{
		Servers: []Server{{ID: peerID, Suffrage: Voter}},
	}, 1)

	s := &followerReplication{
		peer:       Server{ID: peerID},
		commitment: c,
	}
	atomic.StoreUint64(&s.nextIndex, 1)

	appendReq := func(indexes ...uint64) *AppendEntriesRequest {
		req := &AppendEntriesRequest{}
		for _, idx := range indexes {
			req.Entries = append(req.Entries, &Log{Index: idx})
		}
		return req
	}

	updateLastAppended(s, appendReq(1, 2, 3, 4))
	require.Equal(t, uint64(5), atomic.LoadUint64(&s.nextIndex))
	c.Lock()
	require.Equal(t, uint64(4), c.matchIndexes[peerID])
	c.Unlock()

	// Exact duplicate: no movement and no commitment recalculation side effect.
	updateLastAppended(s, appendReq(1, 2, 3, 4))
	require.Equal(t, uint64(5), atomic.LoadUint64(&s.nextIndex))

	// Reordered/older response must not roll nextIndex or match backwards.
	updateLastAppended(s, appendReq(2, 3))
	require.Equal(t, uint64(5), atomic.LoadUint64(&s.nextIndex))
	c.Lock()
	require.Equal(t, uint64(4), c.matchIndexes[peerID])
	c.Unlock()

	// A newer response still advances.
	updateLastAppended(s, appendReq(5, 6))
	require.Equal(t, uint64(7), atomic.LoadUint64(&s.nextIndex))
	c.Lock()
	require.Equal(t, uint64(6), c.matchIndexes[peerID])
	c.Unlock()
}

func TestUID559_PipelineCloseReleasesWindow(t *testing.T) {
	addrA, transA := NewInmemTransport("")
	addrB, transB := NewInmemTransport("")
	transA.Connect(addrB, transB)
	transB.Connect(addrA, transA)
	defer func() { _ = transA.Close() }()
	defer func() { _ = transB.Close() }()

	pipeline, err := transA.AppendEntriesPipeline(ServerID("uid559-follower"), addrB)
	require.NoError(t, err)

	s := &followerReplication{}
	s.initReplicationWindow(1)
	require.True(t, s.acquireInflight(1, nil))

	stopCh := make(chan struct{})
	finishCh := make(chan struct{})
	r := new(Raft)
	go r.pipelineDecode(s, pipeline, stopCh, finishCh)

	require.NoError(t, pipeline.Close())
	select {
	case <-finishCh:
	case <-time.After(time.Second):
		t.Fatal("decoder did not stop after pipeline close")
	}
	require.Eventually(t, func() bool {
		return s.inflightCount() == 0
	}, time.Second, time.Millisecond)

	done := make(chan bool, 1)
	go func() {
		done <- s.waitForWindowCapacity(1, s.abortCh)
	}()
	select {
	case ok := <-done:
		require.True(t, ok)
	case <-time.After(time.Second):
		t.Fatal("window sender was not woken after disconnect")
	}
}

func TestUID559_ClusterSmallWindowReplicates(t *testing.T) {
	conf := inmemConfig(t)
	conf.MaxInflightLogs = 1
	c := MakeCluster(3, t, conf)
	defer c.Close()

	leader := c.Leader()
	for i := 0; i < 25; i++ {
		f := leader.Apply([]byte(fmt.Sprintf("window-%d", i)), 5*time.Second)
		require.NoError(t, f.Error())
	}
	c.EnsureSame(t)

	for id, repl := range leader.leaderState.replState {
		require.Equalf(t, uint64(1), repl.maxWindow(), "peer %s window", id)
		require.Zero(t, repl.inflightCount())
	}
}

func TestUID559_DisconnectResetsWindowAndReconnectResumes(t *testing.T) {
	conf := inmemConfig(t)
	conf.MaxInflightLogs = 4
	c := MakeCluster(3, t, conf)
	defer c.Close()

	leader := c.Leader()
	behind := c.Followers()[0]
	c.Disconnect(behind.localAddr)

	var last Future
	for i := 0; i < 20; i++ {
		last = leader.Apply([]byte(fmt.Sprintf("disconnect-%d", i)), 5*time.Second)
	}
	require.NoError(t, last.Error())

	c.FullyConnect()
	c.EnsureSame(t)

	leader = c.Leader()
	repl := leader.leaderState.replState[behind.localID]
	require.NotNil(t, repl)
	require.Eventually(t, func() bool {
		return repl.inflightCount() == 0
	}, 2*time.Second, 10*time.Millisecond)

	lastIndex, _ := leader.getLastLog()
	require.Equal(t, lastIndex+1, atomic.LoadUint64(&repl.nextIndex))
}

func TestUID559_ProactiveSnapshotCatchup(t *testing.T) {
	conf := inmemConfig(t)
	conf.SnapshotCatchupThreshold = 5
	conf.TrailingLogs = 10
	c := MakeCluster(3, t, conf)
	defer c.Close()

	leader := c.Leader()
	behind := c.Followers()[0]
	c.Disconnect(behind.localAddr)

	var last Future
	for i := 0; i < 25; i++ {
		last = leader.Apply([]byte(fmt.Sprintf("snapshot-window-%d", i)), 5*time.Second)
	}
	require.NoError(t, last.Error())
	require.NoError(t, leader.Snapshot().Error())

	c.FullyConnect()
	c.EnsureSame(t)
}
