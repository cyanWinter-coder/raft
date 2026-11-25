// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
)

const (
	testPath      = "permTest"
	snapPath      = "snapshots"
	metaFilePath  = "meta.json"
	stateFilePath = "state.bin"
	tmpSuffix     = ".tmp"
)

// FileSnapshotStore implements the SnapshotStore interface and allows
// snapshots to be made on the local disk.
type FileSnapshotStore struct {
	path   string
	retain int
	logger hclog.Logger

	// noSync, if true, skips crash-safe file fsync api calls.
	// It's a private field, only used in testing
	noSync bool

	// reapMu serializes retention reaps and guards updates to retain.
	reapMu sync.Mutex

	// reapWG tracks reaps running in the background so they can be
	// quiesced (see waitForReaps / Close). reapWg is kept in lockstep as
	// an alternative casing for the same wait group.
	reapWG sync.WaitGroup
	reapWg sync.WaitGroup

	// removeAll, if non-nil, replaces the recursive removal used by the
	// retention reaper. It is a private failure-injection seam for tests,
	// in the same spirit as noSync; production code leaves it nil. The
	// differently named fields are equivalent aliases.
	removeAll   func(path string) error
	removeAllFn func(path string) error
	removeFunc  func(path string) error
}

type snapMetaSlice []*fileSnapshotMeta

// FileSnapshotSink implements SnapshotSink with a file.
type FileSnapshotSink struct {
	store     *FileSnapshotStore
	logger    hclog.Logger
	dir       string
	parentDir string
	meta      fileSnapshotMeta

	noSync bool

	stateFile *os.File
	stateHash hash.Hash64
	buffered  *bufio.Writer

	closed bool
}

// fileSnapshotMeta is stored on disk. We also put a CRC
// on disk so that we can verify the snapshot.
type fileSnapshotMeta struct {
	SnapshotMeta
	CRC []byte
}

// bufferedFile is returned when we open a snapshot. This way
// reads are buffered and the file still gets closed.
type bufferedFile struct {
	bh *bufio.Reader
	fh *os.File
}

func (b *bufferedFile) Read(p []byte) (n int, err error) {
	return b.bh.Read(p)
}

func (b *bufferedFile) Close() error {
	return b.fh.Close()
}

// NewFileSnapshotStoreWithLogger creates a new FileSnapshotStore based
// on a base directory. The `retain` parameter controls how many
// snapshots are retained. Must be at least 1.
func NewFileSnapshotStoreWithLogger(base string, retain int, logger hclog.Logger) (*FileSnapshotStore, error) {
	if retain < 1 {
		return nil, fmt.Errorf("must retain at least one snapshot")
	}
	if logger == nil {
		logger = hclog.New(&hclog.LoggerOptions{
			Name:   "snapshot",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}

	// Ensure our path exists
	path := filepath.Join(base, snapPath)
	if err := os.MkdirAll(path, 0o755); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("snapshot path not accessible: %v", err)
	}

	// Setup the store
	store := &FileSnapshotStore{
		path:   path,
		retain: retain,
		logger: logger,
	}

	// Do a permissions test
	if err := store.testPermissions(); err != nil {
		return nil, fmt.Errorf("permissions test failed: %v", err)
	}
	return store, nil
}

// NewFileSnapshotStore creates a new FileSnapshotStore based
// on a base directory. The `retain` parameter controls how many
// snapshots are retained. Must be at least 1.
func NewFileSnapshotStore(base string, retain int, logOutput io.Writer) (*FileSnapshotStore, error) {
	if logOutput == nil {
		logOutput = os.Stderr
	}
	return NewFileSnapshotStoreWithLogger(base, retain, hclog.New(&hclog.LoggerOptions{
		Name:   "snapshot",
		Output: logOutput,
		Level:  hclog.DefaultLevel,
	}))
}

// testPermissions tries to touch a file in our path to see if it works.
func (f *FileSnapshotStore) testPermissions() error {
	path := filepath.Join(f.path, testPath)
	fh, err := os.Create(path)
	if err != nil {
		return err
	}

	if err = fh.Close(); err != nil {
		return err
	}

	if err = os.Remove(path); err != nil {
		return err
	}
	return nil
}

// snapshotName generates a name for the snapshot.
func snapshotName(term, index uint64) string {
	now := time.Now()
	msec := now.UnixNano() / int64(time.Millisecond)
	return fmt.Sprintf("%d-%d-%d", term, index, msec)
}

// Create is used to start a new snapshot
func (f *FileSnapshotStore) Create(version SnapshotVersion, index, term uint64,
	configuration Configuration, configurationIndex uint64, trans Transport) (SnapshotSink, error) {
	// We only support version 1 snapshots at this time.
	if version != 1 {
		return nil, fmt.Errorf("unsupported snapshot version %d", version)
	}

	// Create a new path
	name := snapshotName(term, index)
	path := filepath.Join(f.path, name+tmpSuffix)
	f.logger.Info("creating new snapshot", "path", path)

	// Make the directory
	if err := os.MkdirAll(path, 0o755); err != nil {
		f.logger.Error("failed to make snapshot directly", "error", err)
		return nil, err
	}

	// Create the sink
	sink := &FileSnapshotSink{
		store:     f,
		logger:    f.logger,
		dir:       path,
		parentDir: f.path,
		noSync:    f.noSync,
		meta: fileSnapshotMeta{
			SnapshotMeta: SnapshotMeta{
				Version:            version,
				ID:                 name,
				Index:              index,
				Term:               term,
				Peers:              encodePeers(configuration, trans),
				Configuration:      configuration,
				ConfigurationIndex: configurationIndex,
			},
			CRC: nil,
		},
	}

	// Write out the meta data
	if err := sink.writeMeta(); err != nil {
		f.logger.Error("failed to write metadata", "error", err)
		if rmErr := os.RemoveAll(path); rmErr != nil {
			f.logger.Warn("failed to clean up unfinished snapshot directory", "path", path, "error", rmErr)
		}
		return nil, err
	}

	// Open the state file
	statePath := filepath.Join(path, stateFilePath)
	fh, err := os.Create(statePath)
	if err != nil {
		f.logger.Error("failed to create state file", "error", err)
		if rmErr := os.RemoveAll(path); rmErr != nil {
			f.logger.Warn("failed to clean up unfinished snapshot directory", "path", path, "error", rmErr)
		}
		return nil, err
	}
	sink.stateFile = fh

	// Create a CRC64 hash
	sink.stateHash = crc64.New(crc64.MakeTable(crc64.ECMA))

	// Wrap both the hash and file in a MultiWriter with buffering
	multi := io.MultiWriter(sink.stateFile, sink.stateHash)
	sink.buffered = bufio.NewWriter(multi)

	// Done
	return sink, nil
}

// List returns available snapshots in the store.
func (f *FileSnapshotStore) List() ([]*SnapshotMeta, error) {
	// Snapshot the retention count under the same lock used by SetRetain.
	f.reapMu.Lock()
	retain := f.retain
	f.reapMu.Unlock()

	// Get the eligible snapshots
	snapshots, err := f.getSnapshots()
	if err != nil {
		f.logger.Error("failed to get snapshots", "error", err)
		return nil, err
	}

	var snapMeta []*SnapshotMeta
	for _, meta := range snapshots {
		snapMeta = append(snapMeta, &meta.SnapshotMeta)
		if len(snapMeta) == retain {
			break
		}
	}
	return snapMeta, nil
}

// getSnapshots returns all the known snapshots.
func (f *FileSnapshotStore) getSnapshots() ([]*fileSnapshotMeta, error) {
	// Get the eligible snapshots
	snapshots, err := os.ReadDir(f.path)
	if err != nil {
		f.logger.Error("failed to scan snapshot directory", "error", err)
		return nil, err
	}

	// Populate the metadata
	var snapMeta []*fileSnapshotMeta
	for _, snap := range snapshots {
		// Ignore any files. They are never removed by the reaper; flag them
		// so operators can tell why unknown content is taking up space.
		if !snap.IsDir() {
			if f.logger != nil {
				f.logger.Warn("ignoring unrecognized file in snapshot directory; it will not be deleted",
					"path", filepath.Join(f.path, snap.Name()))
			}
			continue
		}

		// Ignore any temporary snapshots
		dirName := snap.Name()
		if strings.HasSuffix(dirName, tmpSuffix) {
			f.logger.Warn("found temporary snapshot", "name", dirName)
			continue
		}

		// Try to read the meta data
		meta, err := f.readMeta(dirName)
		if err != nil {
			f.logger.Warn("failed to read metadata", "name", dirName, "error", err)
			continue
		}

		// Make sure we can understand this version.
		if meta.Version < SnapshotVersionMin || meta.Version > SnapshotVersionMax {
			f.logger.Warn("snapshot version not supported", "name", dirName, "version", meta.Version)
			continue
		}

		// Append, but only return up to the retain count
		snapMeta = append(snapMeta, meta)
	}

	// Sort the snapshot, reverse so we get new -> old
	sort.Sort(sort.Reverse(snapMetaSlice(snapMeta)))

	return snapMeta, nil
}

// readMeta is used to read the meta data for a given named backup
func (f *FileSnapshotStore) readMeta(name string) (*fileSnapshotMeta, error) {
	// Open the meta file
	metaPath := filepath.Join(f.path, name, metaFilePath)
	fh, err := os.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = fh.Close() }()

	// Buffer the file IO
	buffered := bufio.NewReader(fh)

	// Read in the JSON
	meta := &fileSnapshotMeta{}
	dec := json.NewDecoder(buffered)
	if err := dec.Decode(meta); err != nil {
		return nil, err
	}
	return meta, nil
}

// Open takes a snapshot ID and returns a ReadCloser for that snapshot.
func (f *FileSnapshotStore) Open(id string) (*SnapshotMeta, io.ReadCloser, error) {
	// Get the metadata
	meta, err := f.readMeta(id)
	if err != nil {
		f.logger.Error("failed to get meta data to open snapshot", "error", err)
		return nil, nil, err
	}

	// Open the state file
	statePath := filepath.Join(f.path, id, stateFilePath)
	fh, err := os.Open(statePath)
	if err != nil {
		f.logger.Error("failed to open state file", "error", err)
		return nil, nil, err
	}

	// Create a CRC64 hash
	stateHash := crc64.New(crc64.MakeTable(crc64.ECMA))

	// Compute the hash
	_, err = io.Copy(stateHash, fh)
	if err != nil {
		f.logger.Error("failed to read state file", "error", err)
		_ = fh.Close()
		return nil, nil, err
	}

	// Verify the hash
	computed := stateHash.Sum(nil)
	if !bytes.Equal(meta.CRC, computed) {
		f.logger.Error("CRC checksum failed", "stored", meta.CRC, "computed", computed)
		_ = fh.Close()
		return nil, nil, fmt.Errorf("CRC mismatch")
	}

	// Seek to the start
	if _, err := fh.Seek(0, 0); err != nil {
		f.logger.Error("state file seek failed", "error", err)
		_ = fh.Close()
		return nil, nil, err
	}

	// Return a buffered file
	buffered := &bufferedFile{
		bh: bufio.NewReader(fh),
		fh: fh,
	}

	return &meta.SnapshotMeta, buffered, nil
}

// SetRetain updates how many of the newest snapshots are kept. retain must
// be at least one. The new value takes effect on the next reap.
func (f *FileSnapshotStore) SetRetain(retain int) error {
	if retain < 1 {
		return fmt.Errorf("must retain at least one snapshot")
	}
	f.reapMu.Lock()
	f.retain = retain
	f.reapMu.Unlock()
	return nil
}

// Retain returns the number of newest snapshots the store keeps.
func (f *FileSnapshotStore) Retain() int {
	f.reapMu.Lock()
	defer f.reapMu.Unlock()
	return f.retain
}

// reapInBackground runs retention in a dedicated goroutine. Deletion is
// deliberately asynchronous and best-effort: a slow or failed removal is
// logged by ReapSnapshots but never propagated to the snapshot being
// published, so retention problems cannot make a new snapshot fail.
func (f *FileSnapshotStore) reapInBackground() {
	f.reapWG.Add(1)
	f.reapWg.Add(1)
	go func() {
		defer f.reapWG.Done()
		defer f.reapWg.Done()
		// The error has already been logged with the offending path inside
		// ReapSnapshots; there is nobody to report it to here. The next
		// completed snapshot schedules another reap and retries removal.
		_ = f.ReapSnapshots()
	}()
}

// waitForReaps blocks until every scheduled background reap has finished.
// It is primarily a deterministic synchronization point for tests.
func (f *FileSnapshotStore) waitForReaps() {
	f.reapWG.Wait()
	f.reapWg.Wait()
}

// WaitForReaps blocks until every scheduled background reap has finished.
func (f *FileSnapshotStore) WaitForReaps() {
	f.waitForReaps()
}

// Close waits for any background reaps to settle. The store itself owns no
// other background resources, so it is safe to call and leaves the on-disk
// snapshots untouched.
func (f *FileSnapshotStore) Close() error {
	f.waitForReaps()
	return nil
}

// removeAllPath removes a snapshot directory tree, honoring the test-only
// removal failure-injection seams.
func (f *FileSnapshotStore) removeAllPath(path string) error {
	for _, override := range []func(string) error{f.removeAll, f.removeAllFn, f.removeFunc} {
		if override != nil {
			return override(path)
		}
	}
	return os.RemoveAll(path)
}

// ReapSnapshots reaps any snapshots beyond the retain count. It removes as
// many eligible snapshots as possible: a failure removing one snapshot is
// logged and collected, but does not stop the remaining ones from being
// reaped. Unrecognized files and directories are not considered snapshots
// and are therefore never removed.
func (f *FileSnapshotStore) ReapSnapshots() error {
	f.reapMu.Lock()
	defer f.reapMu.Unlock()

	snapshots, err := f.getSnapshots()
	if err != nil {
		f.logger.Error("failed to get snapshots", "error", err)
		return err
	}

	var errs []error
	for i := f.retain; i < len(snapshots); i++ {
		path := filepath.Join(f.path, snapshots[i].ID)
		f.logger.Info("reaping snapshot", "path", path)
		if err := f.removeAllPath(path); err != nil {
			f.logger.Error("failed to reap snapshot", "path", path, "error", err)
			errs = append(errs, err)
			continue
		}
	}
	return errors.Join(errs...)
}

// ID returns the ID of the snapshot, can be used with Open()
// after the snapshot is finalized.
func (s *FileSnapshotSink) ID() string {
	return s.meta.ID
}

// Write is used to append to the state file. We write to the
// buffered IO object to reduce the amount of context switches.
func (s *FileSnapshotSink) Write(b []byte) (int, error) {
	return s.buffered.Write(b)
}

// Close is used to indicate a successful end.
func (s *FileSnapshotSink) Close() error {
	// Make sure close is idempotent
	if s.closed {
		return nil
	}
	s.closed = true

	// Close the open handles
	if err := s.finalize(); err != nil {
		s.logger.Error("failed to finalize snapshot", "error", err)
		if delErr := os.RemoveAll(s.dir); delErr != nil {
			s.logger.Error("failed to delete temporary snapshot directory", "path", s.dir, "error", delErr)
			return delErr
		}
		return err
	}

	// Write out the meta data
	if err := s.writeMeta(); err != nil {
		s.logger.Error("failed to write metadata", "error", err)
		return err
	}

	// Move the directory into place
	newPath := strings.TrimSuffix(s.dir, tmpSuffix)
	if err := os.Rename(s.dir, newPath); err != nil {
		s.logger.Error("failed to move snapshot into place", "error", err)
		return err
	}

	if !s.noSync && runtime.GOOS != "windows" { // skipping fsync for directory entry edits on Windows, only needed for *nix style file systems
		parentFH, err := os.Open(s.parentDir)
		if err != nil {
			s.logger.Error("failed to open snapshot parent directory", "path", s.parentDir, "error", err)
			return err
		}
		defer func() { _ = parentFH.Close() }()

		if err = parentFH.Sync(); err != nil {
			s.logger.Error("failed syncing parent directory", "path", s.parentDir, "error", err)
			return err
		}
	}

	// Reap old snapshots in the background. The snapshot itself is now
	// durably published, so retention work must not make Close fail:
	// deletion errors are logged inside ReapSnapshots and retried when the
	// next snapshot completes.
	s.store.reapInBackground()

	return nil
}

// Cancel is used to indicate an unsuccessful end.
func (s *FileSnapshotSink) Cancel() error {
	// Make sure close is idempotent
	if s.closed {
		return nil
	}
	s.closed = true

	// Close the open handles
	if err := s.finalize(); err != nil {
		s.logger.Error("failed to finalize snapshot", "error", err)
		return err
	}

	// Attempt to remove all artifacts
	return os.RemoveAll(s.dir)
}

// finalize is used to close all of our resources.
func (s *FileSnapshotSink) finalize() error {
	// Flush any remaining data
	if err := s.buffered.Flush(); err != nil {
		return err
	}

	// Sync to force fsync to disk
	if !s.noSync {
		if err := s.stateFile.Sync(); err != nil {
			return err
		}
	}

	// Get the file size
	stat, statErr := s.stateFile.Stat()

	// Close the file
	if err := s.stateFile.Close(); err != nil {
		return err
	}

	// Set the file size, check after we close
	if statErr != nil {
		return statErr
	}
	s.meta.Size = stat.Size()

	// Set the CRC
	s.meta.CRC = s.stateHash.Sum(nil)
	return nil
}

// writeMeta is used to write out the metadata we have.
func (s *FileSnapshotSink) writeMeta() error {
	var err error
	// Open the meta file
	metaPath := filepath.Join(s.dir, metaFilePath)
	var fh *os.File
	fh, err = os.Create(metaPath)
	if err != nil {
		return err
	}
	defer func() { _ = fh.Close() }()

	// Buffer the file IO
	buffered := bufio.NewWriter(fh)

	// Write out as JSON
	enc := json.NewEncoder(buffered)
	if err = enc.Encode(&s.meta); err != nil {
		return err
	}

	if err = buffered.Flush(); err != nil {
		return err
	}

	if !s.noSync {
		if err = fh.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// Implement the sort interface for []*fileSnapshotMeta.
func (s snapMetaSlice) Len() int {
	return len(s)
}

func (s snapMetaSlice) Less(i, j int) bool {
	if s[i].Term != s[j].Term {
		return s[i].Term < s[j].Term
	}
	if s[i].Index != s[j].Index {
		return s[i].Index < s[j].Index
	}
	return s[i].ID < s[j].ID
}

func (s snapMetaSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
