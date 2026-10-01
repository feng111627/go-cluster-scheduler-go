package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Store is the single serialized authority over State.
// Every mutation goes through Update (which atomically persists the whole
// state); every read goes through View. This guarantees that concurrent
// reconcilers can never observe or produce torn/duplicate state.
type Store struct {
	mu       sync.RWMutex
	state    *State
	filePath string
	now      func() time.Time
}

// NewStore creates a store backed by filePath (empty = memory only) and
// loads any previously persisted state so that a process restart continues
// exactly where it left off.
func NewStore(filePath string) (*Store, error) {
	return newStore(filePath, time.Now)
}

func newStore(filePath string, now func() time.Time) (*Store, error) {
	if now == nil {
		now = time.Now
	}
	s := &Store{
		state:    newState(),
		filePath: filePath,
		now:      now,
	}
	if filePath != "" {
		if err := s.load(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func newState() *State {
	return &State{
		Nodes:       map[string]*Node{},
		Pods:        map[string]*Pod{},
		Deployments: map[string]*Deployment{},
	}
}

// View runs fn against the state while holding the read lock. fn MUST NOT
// mutate the state; the lock guarantees it cannot race with an Update.
func (s *Store) View(fn func(*State)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	fn(s.state)
}

// Update runs fn against the live state under the write lock and persists
// atomically afterwards. A non-nil error aborts: in-memory state is rolled
// back and nothing is persisted, so memory and disk always agree.
func (s *Store) Update(fn func(*State) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	backup := s.state.clone()
	if err := fn(s.state); err != nil {
		s.state = backup // roll back aborted mutation
		return err
	}
	s.state.Epoch++
	s.state.UpdatedAt = s.now().UTC()
	if err := s.persist(); err != nil {
		s.state = backup // roll back un-persisted mutation
		return fmt.Errorf("persist state: %w", err)
	}
	return nil
}

// persist writes the whole state to a temp file, fsyncs it and renames it
// over the target, so a crash at any point leaves either the old file or the
// complete new file, never a half-written state file.
func (s *Store) persist() error {
	if s.filePath == "" {
		return nil
	}
	dir := filepath.Dir(s.filePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".state-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil { // fsync data
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, s.filePath); err != nil {
		cleanup()
		return err
	}
	// Best-effort fsync of the directory so the rename itself is durable.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// load reads the state file if it exists. A missing file is not an error.
func (s *Store) load() error {
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if len(data) == 0 {
		return nil
	}
	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		return fmt.Errorf("parse state file %s: %w", s.filePath, err)
	}
	if st.Nodes == nil {
		st.Nodes = map[string]*Node{}
	}
	if st.Pods == nil {
		st.Pods = map[string]*Pod{}
	}
	if st.Deployments == nil {
		st.Deployments = map[string]*Deployment{}
	}
	s.state = &st
	return nil
}

// clone returns a deep copy so failed updates can be rolled back.
func (s *State) clone() *State {
	data, err := json.Marshal(s)
	if err != nil {
		// State contains only JSON-able types; this must never happen.
		panic(fmt.Sprintf("state clone marshal: %v", err))
	}
	var c State
	if err := json.Unmarshal(data, &c); err != nil {
		panic(fmt.Sprintf("state clone unmarshal: %v", err))
	}
	return &c
}
