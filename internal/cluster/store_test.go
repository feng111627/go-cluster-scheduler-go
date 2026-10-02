package cluster

import (
	"path/filepath"
	"sync"
	"testing"
)

func TestStore_PersistAndReload(t *testing.T) {
	file := filepath.Join(t.TempDir(), "state.json")
	s1, err := NewStore(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.UpsertNode(&Node{Name: "n1", Capacity: Resources{CPU: 1000, Memory: 256}, Ready: true}); err != nil {
		t.Fatal(err)
	}
	if err := s1.Update(func(st *State) error {
		st.Pods["p1"] = &Pod{UID: "p1", Phase: PhaseRunning, NodeName: "n1"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var epochBefore int64
	s1.View(func(st *State) { epochBefore = st.Epoch })

	// Reload from disk: everything must be present and epoch continues.
	s2, err := NewStore(file)
	if err != nil {
		t.Fatal(err)
	}
	s2.View(func(st *State) {
		if st.Nodes["n1"] == nil {
			t.Fatal("node n1 not restored from disk")
		}
		if st.Pods["p1"] == nil || st.Pods["p1"].NodeName != "n1" {
			t.Fatalf("pod p1 not restored correctly: %+v", st.Pods["p1"])
		}
		if st.Epoch < epochBefore {
			t.Fatalf("epoch regressed after reload: %d < %d", st.Epoch, epochBefore)
		}
	})
}

func TestStore_FailedUpdateRollsBack(t *testing.T) {
	s, _ := NewStore("")
	if err := s.UpsertNode(&Node{Name: "n1", Capacity: Resources{CPU: 1000, Memory: 256}, Ready: true}); err != nil {
		t.Fatal(err)
	}
	boom := s.Update(func(st *State) error {
		st.Nodes["ghost"] = &Node{Name: "ghost"}
		return errBoom
	})
	if boom == nil {
		t.Fatal("expected error from aborted update")
	}
	s.View(func(st *State) {
		if st.Nodes["ghost"] != nil {
			t.Fatal("aborted mutation must be rolled back, ghost node persisted in memory")
		}
	})
}

// TestStore_ConcurrentUpdatesSerializes hammers the store from many goroutines;
// the store lock must serialize every mutation without a data race (run with
// -race to make this meaningful).
func TestStore_ConcurrentUpdatesSerializes(t *testing.T) {
	s, _ := NewStore(filepath.Join(t.TempDir(), "s.json"))
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if err := s.Update(func(st *State) error {
					st.PodSeq++
					return nil
				}); err != nil {
					t.Errorf("update: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	s.View(func(st *State) {
		if st.PodSeq != 32*20 {
			t.Fatalf("podSeq = %d, want %d (lost updates under concurrency)", st.PodSeq, 32*20)
		}
	})
}

// TestStore_MaintenanceFlagPersists covers the durable planned-maintenance
// flag and the distinguishable wait reason: both must survive a reload so a
// restart during a maintenance window "continues" rather than forgets.
func TestStore_MaintenanceFlagPersists(t *testing.T) {
	file := filepath.Join(t.TempDir(), "state.json")
	s1, err := NewStore(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.UpsertNode(&Node{Name: "n1", Capacity: Resources{CPU: 4000, Memory: 512}, Ready: true}); err != nil {
		t.Fatal(err)
	}
	changed, err := s1.SetMaintenanceFlag("n1", true)
	if err != nil || !changed {
		t.Fatalf("set on: changed=%v err=%v", changed, err)
	}
	if err := s1.Update(func(st *State) error {
		st.Pods["p1"] = &Pod{UID: "p1", Phase: PhaseRescheduling, OriginNode: "n1", WaitReason: WaitReasonDraining}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Idempotent: setting the same value again reports no transition.
	if changed, err := s1.SetMaintenanceFlag("n1", true); err != nil || changed {
		t.Fatalf("re-set on: changed=%v err=%v, want no-op", changed, err)
	}

	s2, err := NewStore(file)
	if err != nil {
		t.Fatal(err)
	}
	s2.View(func(st *State) {
		if !st.Nodes["n1"].UnderMaintenance {
			t.Fatal("maintenance flag must survive restart (cordon cannot be forgotten)")
		}
		if st.Pods["p1"].WaitReason != WaitReasonDraining {
			t.Fatalf("wait reason = %q, want DrainingForMaintenance", st.Pods["p1"].WaitReason)
		}
	})

	// Turning it off is itself persisted.
	if changed, err := s2.SetMaintenanceFlag("n1", false); err != nil || !changed {
		t.Fatalf("set off: changed=%v err=%v", changed, err)
	}
	s3, err := NewStore(file)
	if err != nil {
		t.Fatal(err)
	}
	s3.View(func(st *State) {
		if st.Nodes["n1"].UnderMaintenance {
			t.Fatal("maintenance flag must stay cleared after reload")
		}
	})
}

var errBoom = newSentinel("boom")

type sentinel string

func newSentinel(s string) error { return sentinel(s) }
func (e sentinel) Error() string { return string(e) }
