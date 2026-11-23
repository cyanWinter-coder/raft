// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"
)

// makeQuietTransport builds a TCP transport with a null logger. Transport
// goroutines outlive a test function and would race the testing.T logger if
// it were used here.
func makeQuietTransport(t *testing.T, addressOverride string) *NetworkTransport {
	t.Helper()
	trans, err := NewTCPTransportWithConfig("localhost:0", nil, &NetworkTransportConfig{
		MaxPool:               2,
		MaxRPCsInFlight:       130,
		Timeout:               time.Second,
		Logger:                hclog.NewNullLogger(),
		ServerAddressProvider: &testAddrProvider{addressOverride},
	})
	require.NoError(t, err)
	return trans
}

// TestPipelineTransport_CloseCompletesInflight verifies the transport-level
// contract the replication layer relies on: closing a pipeline completes every
// request that was accepted, exactly once, with ErrPipelineShutdown, on both
// the in-memory and network transports. It also verifies that a second Close
// is a no-op and that new sends are refused rather than hanging.
func TestPipelineTransport_CloseCompletesInflight(t *testing.T) {
	run := func(t *testing.T, open func(t *testing.T) AppendPipeline) {
		t.Helper()
		pipeline := open(t)

		const n = 8
		futures := make([]AppendFuture, 0, n)
		for i := 0; i < n; i++ {
			fut, err := pipeline.AppendEntries(
				&AppendEntriesRequest{Term: 1, PrevLogEntry: uint64(i)},
				new(AppendEntriesResponse),
			)
			require.NoError(t, err)
			futures = append(futures, fut)
		}

		// Close while every request is still outstanding: no responder was
		// started, so none of them can have completed normally yet.
		require.NoError(t, pipeline.Close())

		for i, fut := range futures {
			errCh := make(chan error, 1)
			go func() { errCh <- fut.Error() }()
			select {
			case err := <-errCh:
				require.ErrorIs(t, err, ErrPipelineShutdown, "future %d", i)
			case <-time.After(time.Second):
				t.Fatalf("future %d never completed after Close", i)
			}
			// Repeated reads return the same terminal result.
			require.ErrorIs(t, fut.Error(), ErrPipelineShutdown)
		}

		// Close is idempotent.
		require.NoError(t, pipeline.Close())

		// New sends are refused.
		_, err := pipeline.AppendEntries(&AppendEntriesRequest{}, new(AppendEntriesResponse))
		require.ErrorIs(t, err, ErrPipelineShutdown)
	}

	t.Run("inmem", func(t *testing.T) {
		run(t, func(t *testing.T) AppendPipeline {
			addr1, trans1 := NewInmemTransport("")
			addr2, trans2 := NewInmemTransport("")
			t.Cleanup(func() {
				_ = trans1.Close()
				_ = trans2.Close()
			})
			trans1.Connect(addr2, trans2)
			trans2.Connect(addr1, trans1)
			// trans1 never consumes, so the requests stay outstanding.
			p, err := trans2.AppendEntriesPipeline("id1", addr1)
			require.NoError(t, err)
			return p
		})
	})

	t.Run("network", func(t *testing.T) {
		run(t, func(t *testing.T) AppendPipeline {
			srv := makeQuietTransport(t, "localhost:0")
			t.Cleanup(func() { _ = srv.Close() })
			// The server never consumes from Consumer, so requests remain
			// outstanding on the stream.
			cli := makeQuietTransport(t, string(srv.LocalAddr()))
			t.Cleanup(func() { _ = cli.Close() })
			p, err := cli.AppendEntriesPipeline("id1", srv.LocalAddr())
			require.NoError(t, err)
			return p
		})
	})
}
