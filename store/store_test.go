package store

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/feng111627/go-cluster-scheduler-go/api"
)

func TestStore_PersistAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	ctx := context.Background()

	st, err := New(ctx, path)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	err = st.Update(func(s *api.Snapshot) error {
		s.Nodes["n1"] = &api.Node{Name: "n1", Phase: api.NodeReady}
		s.Pods["pod-000001"] = &api.Pod{UID: "pod-000001", Phase: api.PodRunning, NodeName: "n1"}
		return nil
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if got := st.Gen(); got != 2 { // initial create + one update
		t.Fatalf("gen = %d, want 2", got)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// "Process restart": reopen the same file and assert state survived.
	st2, err := New(ctx, path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	st2.View(func(s *api.Snapshot) {
		if _, ok := s.Nodes["n1"]; !ok {
			t.Errorf("node n1 missing after reopen")
		}
		if p, ok := s.Pods["pod-000001"]; !ok || p.NodeName != "n1" || p.Phase != api.PodRunning {
			t.Errorf("pod not restored correctly: %+v", p)
		}
		if s.Gen != 2 {
			t.Errorf("gen after reopen = %d, want 2", s.Gen)
		}
	})
}

func TestStore_RollbackOnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	ctx := context.Background()
	st, err := New(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	wantErr := context.Canceled
	if err := st.Update(func(s *api.Snapshot) error {
		s.Nodes["ghost"] = &api.Node{Name: "ghost"}
		return wantErr
	}); err != wantErr {
		t.Fatalf("want canceled, got %v", err)
	}
	st.View(func(s *api.Snapshot) {
		if _, ok := s.Nodes["ghost"]; ok {
			t.Fatal("rolled-back node persisted")
		}
	})
}

func TestStore_ConcurrentUpdatesAreSerializable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	ctx := context.Background()
	st, err := New(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	const writers = 32
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = st.Update(func(s *api.Snapshot) error {
				s.NextPodSeq++
				return nil
			})
		}(i)
	}
	wg.Wait()
	// Reopen to verify the last persisted value matches: no lost update.
	st2, err := New(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	st2.View(func(s *api.Snapshot) {
		// gen started at 1 (create), each successful update adds 1.
		if s.Gen != 1+int64(writers) {
			t.Errorf("gen = %d, want %d (lost updates?)", s.Gen, 1+writers)
		}
		if s.NextPodSeq != writers {
			t.Errorf("NextPodSeq = %d, want %d (lost updates?)", s.NextPodSeq, writers)
		}
	})
}
