// Package store provides a durable, mutex-protected, atomically-written
// JSON store of cluster state. All state mutation goes through Update so
// that concurrent reconcilers can never observe or persist interleaved
// writes, and a process restart resumes from the last committed snapshot.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/feng111627/go-cluster-scheduler-go/api"
)

// Store persists cluster state to a local JSON file.
type Store struct {
	path  string
	mu    sync.RWMutex
	snap  *api.Snapshot
	fresh bool // true when New had to create the file
}

// New opens (or creates) the store at path and loads any existing state.
// A freshly created store contains empty, non-nil maps.
func New(ctx context.Context, path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("store: empty path")
	}
	s := &Store{path: path, snap: newSnapshot()}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if len(data) > 0 {
			var loaded api.Snapshot
			if err := json.Unmarshal(data, &loaded); err != nil {
				return nil, fmt.Errorf("store: decode %s: %w", path, err)
			}
			ensureMaps(&loaded)
			s.snap = &loaded
		}
	case os.IsNotExist(err):
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("store: mkdir: %w", err)
		}
		// First committed generation of the empty store.
		s.snap.Gen = 1
		s.fresh = true
		if err := s.persist(s.snap); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("store: read %s: %w", path, err)
	}
	return s, nil
}

func newSnapshot() *api.Snapshot {
	snap := &api.Snapshot{}
	ensureMaps(snap)
	return snap
}

func ensureMaps(s *api.Snapshot) {
	if s.Nodes == nil {
		s.Nodes = map[string]*api.Node{}
	}
	if s.Pods == nil {
		s.Pods = map[string]*api.Pod{}
	}
	if s.ReplicaSets == nil {
		s.ReplicaSets = map[string]*api.ReplicaSet{}
	}
}

// Close releases the store.
func (s *Store) Close() error { return nil }

// View executes fn against the snapshot under a read lock. The snapshot
// must not be mutated and must not escape the callback.
func (s *Store) View(fn func(*api.Snapshot)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	fn(s.snap)
}

// Update executes fn against a mutable copy of the snapshot under an
// exclusive lock. If fn returns nil the copy is committed atomically to disk
// and becomes the live snapshot; if fn returns an error the copy is
// discarded, the live snapshot is untouched, and nothing is persisted.
func (s *Store) Update(fn func(*api.Snapshot) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	work := cloneSnapshot(s.snap)
	if err := fn(work); err != nil {
		return err
	}
	work.Gen++
	if err := s.persist(work); err != nil {
		return err
	}
	s.snap = work
	return nil
}

// cloneSnapshot returns a deep-enough copy so a failed callback can never
// mutate the live snapshot.
func cloneSnapshot(in *api.Snapshot) *api.Snapshot {
	data, err := json.Marshal(in)
	if err != nil {
		// All snapshot content is JSON-able; marshal cannot realistically fail.
		panic(fmt.Errorf("store: clone marshal: %w", err))
	}
	var out api.Snapshot
	if err := json.Unmarshal(data, &out); err != nil {
		panic(fmt.Errorf("store: clone unmarshal: %w", err))
	}
	ensureMaps(&out)
	return &out
}

// persist writes the given snapshot to a temp file and renames it over the
// target, so a crash at any point leaves either the old or the new file,
// never a torn one. Caller must hold mu.
func (s *Store) persist(snap *api.Snapshot) error {
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("store: encode: %w", err)
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".state-*.tmp")
	if err != nil {
		return fmt.Errorf("store: create temp: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("store: write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("store: fsync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("store: close temp: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		cleanup()
		return fmt.Errorf("store: rename: %w", err)
	}
	return nil
}

// Gen returns the current persisted generation counter (useful in logs).
func (s *Store) Gen() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snap.Gen
}

// Fresh reports whether New had to create the file (true) or opened an
// existing persisted state (false).
func (s *Store) Fresh() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fresh
}
