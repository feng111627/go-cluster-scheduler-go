package scheduler

import (
	"errors"
	"sync"
	"testing"

	"github.com/feng111627/go-cluster-scheduler-go/internal/cluster"
	"github.com/feng111627/go-cluster-scheduler-go/internal/testutil"
)

func q(v int64) cluster.Quantity { return cluster.Quantity(v) }

// addNode registers an in-memory node for a test.
func addNode(t *testing.T, st *cluster.Store, n *cluster.Node) {
	t.Helper()
	if err := st.UpsertNode(n); err != nil {
		t.Fatalf("upsert node %s: %v", n.Name, err)
	}
}

// addPending creates a pending pod directly in the store for scheduler tests.
func addPending(t *testing.T, st *cluster.Store, p *cluster.Pod) {
	t.Helper()
	p.Phase = cluster.PhasePending
	err := st.Update(func(s *cluster.State) error {
		s.Pods[p.UID] = p
		return nil
	})
	if err != nil {
		t.Fatalf("add pod %s: %v", p.UID, err)
	}
}

func newTestStore(t *testing.T) *cluster.Store {
	t.Helper()
	st, err := cluster.NewStore("") // memory only
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return st
}

func TestSchedule_DeterministicNodeChoice(t *testing.T) {
	st := newTestStore(t)
	sc := New(st, testutil.Log{T: t})

	// Two identical nodes: tie broken by lexicographically smallest name.
	addNode(t, st, &cluster.Node{Name: "node-b", Capacity: cluster.Resources{CPU: q(4000), Memory: 4096}, Ready: true})
	addNode(t, st, &cluster.Node{Name: "node-a", Capacity: cluster.Resources{CPU: q(4000), Memory: 4096}, Ready: true})

	addPending(t, st, &cluster.Pod{UID: "p1", Name: "p1", Requests: cluster.Resources{CPU: q(1000), Memory: 512}})
	if err := sc.ScheduleOne("p1"); err != nil {
		t.Fatalf("ScheduleOne: %v", err)
	}
	st.View(func(s *cluster.State) {
		p := s.Pods["p1"]
		if p.NodeName != "node-a" {
			t.Fatalf("deterministic tie-break: got node %q, want node-a", p.NodeName)
		}
		if p.Phase != cluster.PhaseRunning {
			t.Fatalf("phase = %s, want Running", p.Phase)
		}
		if s.Nodes["node-a"].Used.CPU != q(1000) {
			t.Fatalf("node-a used CPU = %d, want 1000", s.Nodes["node-a"].Used.CPU)
		}
	})

	// Second pod: node-a now has less remaining CPU, so node-b wins on the
	// "most remaining CPU" rule even though its name sorts later.
	addPending(t, st, &cluster.Pod{UID: "p2", Name: "p2", Requests: cluster.Resources{CPU: q(1000), Memory: 512}})
	if err := sc.ScheduleOne("p2"); err != nil {
		t.Fatalf("ScheduleOne p2: %v", err)
	}
	st.View(func(s *cluster.State) {
		if got := s.Pods["p2"].NodeName; got != "node-b" {
			t.Fatalf("balancing rule: got node %q, want node-b", got)
		}
	})
}

func TestSchedule_InsufficientCPU(t *testing.T) {
	st := newTestStore(t)
	sc := New(st, testutil.Log{T: t})
	addNode(t, st, &cluster.Node{Name: "small", Capacity: cluster.Resources{CPU: q(1000), Memory: 8192}, Ready: true})

	addPending(t, st, &cluster.Pod{UID: "big", Name: "big", Requests: cluster.Resources{CPU: q(2000), Memory: 512}})
	err := sc.ScheduleOne("big")

	var se *ScheduleError
	if !errors.As(err, &se) {
		t.Fatalf("want *ScheduleError, got %v", err)
	}
	if got := se.AggregateReason(); got != ReasonInsufficientCPU {
		t.Fatalf("aggregate reason = %s, want %s", got, ReasonInsufficientCPU)
	}
	if se.Failures[0].Reason != ReasonInsufficientCPU {
		t.Fatalf("per-node reason = %s, want InsufficientCPU", se.Failures[0].Reason)
	}
	t.Log(testutil.Describe("CPU rejection evidence", "pod", se.PodUID, "node", se.Failures[0].Node, "detail", se.Failures[0].Detail))
}

func TestSchedule_InsufficientMemory(t *testing.T) {
	st := newTestStore(t)
	sc := New(st, testutil.Log{T: t})
	addNode(t, st, &cluster.Node{Name: "small", Capacity: cluster.Resources{CPU: q(8000), Memory: 1024}, Ready: true})

	addPending(t, st, &cluster.Pod{UID: "membig", Name: "membig", Requests: cluster.Resources{CPU: q(100), Memory: 2048}})
	err := sc.ScheduleOne("membig")

	var se *ScheduleError
	if !errors.As(err, &se) {
		t.Fatalf("want *ScheduleError, got %v", err)
	}
	// CPU fits, so the first (and only) rejection must be the memory reason.
	if se.Failures[0].Reason != ReasonInsufficientMemory {
		t.Fatalf("per-node reason = %s, want InsufficientMemory", se.Failures[0].Reason)
	}
	if got := se.AggregateReason(); got != ReasonInsufficientMemory {
		t.Fatalf("aggregate reason = %s, want InsufficientMemory", got)
	}
	t.Log(testutil.Describe("memory rejection evidence", "detail", se.Failures[0].Detail))
}

func TestSchedule_TaintRejectionAndToleration(t *testing.T) {
	st := newTestStore(t)
	sc := New(st, testutil.Log{T: t})
	addNode(t, st, &cluster.Node{
		Name: "gpu", Capacity: cluster.Resources{CPU: q(8000), Memory: 8192}, Ready: true,
		Taints: []cluster.Taint{{Key: "gpu", Value: "true", Effect: cluster.TaintNoSchedule}},
	})

	// Untolerated pod must be rejected with TaintNotTolerated.
	addPending(t, st, &cluster.Pod{UID: "plain", Name: "plain", Requests: cluster.Resources{CPU: q(100), Memory: 64}})
	err := sc.ScheduleOne("plain")
	var se *ScheduleError
	if !errors.As(err, &se) || se.AggregateReason() != ReasonTaintNotTolerated {
		t.Fatalf("want TaintNotTolerated, got %v", err)
	}
	t.Log(testutil.Describe("taint rejection evidence", "detail", se.Failures[0].Detail))

	// A pod with a matching toleration must be admitted onto the tainted node.
	addPending(t, st, &cluster.Pod{
		UID: "gpu-pod", Name: "gpu-pod",
		Requests:    cluster.Resources{CPU: q(100), Memory: 64},
		Tolerations: []cluster.Toleration{{Key: "gpu", Operator: "Equal", Value: "true", Effect: "NoSchedule"}},
	})
	if err := sc.ScheduleOne("gpu-pod"); err != nil {
		t.Fatalf("tolerated pod ScheduleOne: %v", err)
	}
	st.View(func(s *cluster.State) {
		if s.Pods["gpu-pod"].NodeName != "gpu" || s.Pods["gpu-pod"].Phase != cluster.PhaseRunning {
			t.Fatalf("tolerated pod not bound to gpu node: %+v", s.Pods["gpu-pod"])
		}
	})
}

func TestTolerates(t *testing.T) {
	taint := cluster.Taint{Key: "zone", Value: "edge", Effect: cluster.TaintNoSchedule}
	cases := []struct {
		name string
		tols []cluster.Toleration
		want bool
	}{
		{"none", nil, false},
		{"equal match", []cluster.Toleration{{Key: "zone", Value: "edge", Effect: "NoSchedule"}}, true},
		{"equal default operator", []cluster.Toleration{{Key: "zone", Value: "edge"}}, true},
		{"exists match", []cluster.Toleration{{Key: "zone", Operator: "Exists"}}, true},
		{"wrong value", []cluster.Toleration{{Key: "zone", Value: "core", Effect: "NoSchedule"}}, false},
		{"wrong key", []cluster.Toleration{{Key: "rack", Value: "edge", Effect: "NoSchedule"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tolerates(tc.tols, taint); got != tc.want {
				t.Fatalf("tolerates = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSchedule_NotReadyNode(t *testing.T) {
	st := newTestStore(t)
	sc := New(st, testutil.Log{T: t})
	addNode(t, st, &cluster.Node{Name: "dead", Capacity: cluster.Resources{CPU: q(8000), Memory: 8192}, Ready: false})
	addPending(t, st, &cluster.Pod{UID: "p", Name: "p", Requests: cluster.Resources{CPU: q(100), Memory: 64}})
	err := sc.ScheduleOne("p")
	var se *ScheduleError
	if !errors.As(err, &se) || se.Failures[0].Reason != ReasonNodeNotReady {
		t.Fatalf("want NodeNotReady, got %v", err)
	}
}

// TestSchedule_ConcurrentNoOversell hammers one capacity-3 node with 10 pods
// scheduled concurrently. Exactly 3 must bind and node usage must never exceed
// capacity (run with -race). All evaluation+binding happens inside one
// serialized store update.
func TestSchedule_ConcurrentNoOversell(t *testing.T) {
	st := newTestStore(t)
	sc := New(st, testutil.Log{T: t})
	addNode(t, st, &cluster.Node{Name: "only", Capacity: cluster.Resources{CPU: q(3000), Memory: 8192}, Ready: true})

	for i := 0; i < 10; i++ {
		addPending(t, st, &cluster.Pod{
			UID: fmtTestUID(i), Name: fmtTestUID(i),
			Requests: cluster.Resources{CPU: q(1000), Memory: 128},
		})
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	bound, rejected := 0, 0
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := sc.ScheduleOne(fmtTestUID(i))
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				bound++
			} else {
				rejected++
			}
		}(i)
	}
	wg.Wait()

	if bound != 3 || rejected != 7 {
		t.Fatalf("bound=%d rejected=%d, want 3 bound / 7 rejected (no oversell)", bound, rejected)
	}
	st.View(func(s *cluster.State) {
		if s.Nodes["only"].Used.CPU != q(3000) {
			t.Fatalf("used CPU = %d, want exactly 3000 (capacity)", s.Nodes["only"].Used.CPU)
		}
	})
}

func fmtTestUID(i int) string { return "cp" + string(rune('0'+i)) }
