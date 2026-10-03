package controller

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/feng111627/go-cluster-scheduler-go/internal/cluster"
	"github.com/feng111627/go-cluster-scheduler-go/internal/scheduler"
	"github.com/feng111627/go-cluster-scheduler-go/internal/testutil"
)

func q64(v int64) cluster.Quantity { return cluster.Quantity(v) }

type fixture struct {
	st  *cluster.Store
	sc  *scheduler.Scheduler
	ctl *Controller
	clk *fakeClock
}

type fakeClock struct{ t time.Time }

func (f *fakeClock) now() time.Time { return f.t }
func (f *fakeClock) advance(d time.Duration) time.Time {
	f.t = f.t.Add(d)
	return f.t
}

func newFixture(t *testing.T, stateFile string) *fixture {
	t.Helper()
	clk := &fakeClock{t: time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)}

	// Store with an injected clock: NewStore only takes a path, so drive
	// heartbeats explicitly via HeartbeatAt in tests instead.
	st, err := cluster.NewStore(stateFile)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	sc := scheduler.New(st, testutil.Log{T: t})
	cfg := Config{HeartbeatTimeout: 10 * time.Second, EvictionGrace: 30 * time.Second}
	ctl := NewWithClock(st, sc, cfg, testutil.Log{T: t}, clk.now)
	return &fixture{st: st, sc: sc, ctl: ctl, clk: clk}
}

func (f *fixture) addNode(t *testing.T, name string, cpu, mem int64, taints ...cluster.Taint) {
	t.Helper()
	n := &cluster.Node{
		Name: name, Capacity: cluster.Resources{CPU: q64(cpu), Memory: cluster.Quantity(mem)},
		Ready: true, LastSeen: f.clk.now(), Taints: taints,
	}
	if err := f.st.UpsertNode(n); err != nil {
		t.Fatalf("upsert node: %v", err)
	}
}

func (f *fixture) addDeployment(t *testing.T, name string, replicas int, cpu, mem int64, tols ...cluster.Toleration) {
	t.Helper()
	d := &cluster.Deployment{
		Name: name, Namespace: "default", Replicas: replicas,
		Template: cluster.PodTemplate{
			NamePrefix:  name,
			Requests:    cluster.Resources{CPU: q64(cpu), Memory: cluster.Quantity(mem)},
			Tolerations: tols,
		},
	}
	if err := f.st.UpsertDeployment(d); err != nil {
		t.Fatalf("upsert deployment: %v", err)
	}
}

// phaseCount counts non-terminal pods for a deployment and returns a
// phase->count map plus the grand total (the explainable instance count).
func (f *fixture) phaseCount(t *testing.T) (map[cluster.PodPhase]int, int) {
	t.Helper()
	counts := map[cluster.PodPhase]int{}
	total := 0
	f.st.View(func(s *cluster.State) {
		for _, p := range s.Pods {
			if p.Phase == cluster.PhaseFailed {
				continue
			}
			counts[p.Phase]++
			total++
		}
	})
	return counts, total
}

func mustReconcile(t *testing.T, ctl *Controller, name string) *ReconcileResult {
	t.Helper()
	res, err := ctl.ReconcileDeployment(name)
	var rerr *ReconcileError
	if errors.As(err, &rerr) {
		return res // unschedulable is a legitimate outcome in some tests
	}
	if err != nil {
		t.Fatalf("reconcile %s: %v", name, err)
	}
	return res
}

func TestReconcile_CreatesAndStopsAtDesired(t *testing.T) {
	f := newFixture(t, "")
	f.addNode(t, "n1", 4000, 4096)
	f.addNode(t, "n2", 4000, 4096)
	f.addDeployment(t, "web", 3, 1000, 512)

	res := mustReconcile(t, f.ctl, "web")
	if res.Desired != 3 || res.Active != 0 || len(res.Created) != 3 || len(res.Bound) != 3 {
		t.Fatalf("first reconcile = %+v, want 3 created/bound", res)
	}

	// Repeated reconcile is a no-op: stable identities, deficit recomputed.
	res2 := mustReconcile(t, f.ctl, "web")
	if len(res2.Created) != 0 || len(res2.Bound) != 0 || res2.Active != 3 {
		t.Fatalf("repeat reconcile = %+v, want no creation/binding", res2)
	}
	counts, total := f.phaseCount(t)
	if total != 3 || counts[cluster.PhaseRunning] != 3 {
		t.Fatalf("counts=%v total=%d, want 3 running", counts, total)
	}
}

func TestReconcile_ConcurrentNoDuplicates(t *testing.T) {
	f := newFixture(t, "")
	f.addNode(t, "n1", 8000, 8192)
	f.addNode(t, "n2", 8000, 8192)
	f.addDeployment(t, "web", 10, 100, 64)

	// Hit reconcile from many goroutines simultaneously.
	const workers = 16
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = mustReconcile(t, f.ctl, "web")
		}()
	}
	wg.Wait()

	counts, total := f.phaseCount(t)
	if total != 10 {
		t.Fatalf("concurrent reconcile total = %d, want exactly 10 (no duplicates/over-create)", total)
	}
	if counts[cluster.PhaseRunning] != 10 {
		t.Fatalf("running = %d, want 10", counts[cluster.PhaseRunning])
	}
	// Exactly ordinal identities 0..9 must exist.
	for i := 0; i < 10; i++ {
		uid := fmt.Sprintf("default-web-%d", i)
		f.st.View(func(s *cluster.State) {
			if s.Pods[uid] == nil {
				t.Errorf("missing stable replica %s", uid)
			}
		})
	}
}

func TestReconcile_InsufficientCapacityLeavesPending(t *testing.T) {
	f := newFixture(t, "")
	f.addNode(t, "n1", 2000, 8192) // fits 2 pods of 1000m CPU
	f.addDeployment(t, "web", 5, 1000, 512)

	_, err := f.ctl.ReconcileDeployment("web")
	var rerr *ReconcileError
	if !errors.As(err, &rerr) {
		t.Fatalf("want ReconcileError for unschedulable deficit, got %v", err)
	}
	if len(rerr.ScheduleErrors) != 3 {
		t.Fatalf("unschedulable pods = %d, want 3", len(rerr.ScheduleErrors))
	}
	for _, se := range rerr.ScheduleErrors {
		if se.AggregateReason() != scheduler.ReasonInsufficientCPU {
			t.Fatalf("aggregate = %s, want InsufficientCPU", se.AggregateReason())
		}
	}
	counts, total := f.phaseCount(t)
	// Deficit pods still exist as Pending: total always equals desired.
	if total != 5 || counts[cluster.PhaseRunning] != 2 || counts[cluster.PhasePending] != 3 {
		t.Fatalf("counts=%v total=%d, want 2 running + 3 pending = 5", counts, total)
	}

	// A later reconcile must not create duplicates for the pending deficit.
	mustReconcile(t, f.ctl, "web")
	_, total = f.phaseCount(t)
	if total != 5 {
		t.Fatalf("after repeat reconcile total = %d, want 5", total)
	}
}

func TestReconcile_RestartContinuesFromDisk(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "state.json")

	func() {
		f := newFixture(t, file)
		f.addNode(t, "n1", 4000, 4096)
		f.addDeployment(t, "web", 3, 1000, 512)
		mustReconcile(t, f.ctl, "web")
	}()

	// "Process restart": brand-new store/controller loaded from the file.
	f2 := newFixture(t, file)
	res := mustReconcile(t, f2.ctl, "web")
	if len(res.Created) != 0 {
		t.Fatalf("after restart, created %d replicas; want 0 (state must continue, not restart)", len(res.Created))
	}
	counts, total := f2.phaseCount(t)
	if total != 3 || counts[cluster.PhaseRunning] != 3 {
		t.Fatalf("after restart counts=%v total=%d, want 3 running", counts, total)
	}

	// Scaling up after restart reuses the sequence and does not rebuild.
	if err := f2.st.ScaleDeployment("web", 5); err != nil {
		t.Fatal(err)
	}
	res2 := mustReconcile(t, f2.ctl, "web")
	if len(res2.Created) != 2 {
		t.Fatalf("scale-up created %d, want 2", len(res2.Created))
	}
	_, total = f2.phaseCount(t)
	if total != 5 {
		t.Fatalf("after scale-up total = %d, want 5", total)
	}
}

func TestNodeFailure_FlapDoesNotEvict(t *testing.T) {
	f := newFixture(t, "")
	f.addNode(t, "n1", 4000, 4096)
	f.addNode(t, "n2", 4000, 4096)
	f.addDeployment(t, "web", 4, 1000, 512)
	mustReconcile(t, f.ctl, "web")

	// n1 goes silent for 15s (past 10s timeout, within 30s grace).
	f.clk.advance(15 * time.Second)
	f.st.HeartbeatAt("n2", f.clk.now()) // n2 stays healthy
	ev, err := f.ctl.DetectNodeFailures()
	if err != nil || len(ev) != 0 {
		t.Fatalf("within grace: evicted=%v err=%v, want none", ev, err)
	}
	var n1Ready, n1Evicted bool
	f.st.View(func(s *cluster.State) {
		n1Ready = s.Nodes["n1"].Ready
		n1Evicted = s.Nodes["n1"].Evicted
	})
	if n1Ready {
		t.Fatal("n1 should be marked unreachable")
	}
	if n1Evicted {
		t.Fatal("n1 must not be evicted within grace")
	}

	// Brief flap: heartbeat arrives before grace elapses -> Ready again.
	f.st.HeartbeatAt("n1", f.clk.now())
	if _, err := f.ctl.DetectNodeFailures(); err != nil {
		t.Fatal(err)
	}
	f.st.View(func(s *cluster.State) {
		if !s.Nodes["n1"].Ready || s.Nodes["n1"].Evicted {
			t.Fatal("n1 should have recovered with no eviction/flag")
		}
	})
	counts, total := f.phaseCount(t)
	if total != 4 || counts[cluster.PhaseRunning] != 4 {
		t.Fatalf("after flap counts=%v total=%d, want 4 running (no churn)", counts, total)
	}
}

func TestNodeFailure_EvictionAndReschedule(t *testing.T) {
	f := newFixture(t, "")
	// n1 big, n2 small: after n1 is lost only n2's capacity remains.
	f.addNode(t, "n1", 8000, 8192)
	f.addNode(t, "n2", 2000, 8192)
	f.addDeployment(t, "web", 4, 1000, 512)
	mustReconcile(t, f.ctl, "web")

	// n1 silent beyond grace.
	f.clk.advance(45 * time.Second)
	f.st.HeartbeatAt("n2", f.clk.now())
	ev, err := f.ctl.DetectNodeFailures()
	if err != nil {
		t.Fatalf("detect: %v", err)
	}

	// Determine how many were on n1 and assert the count matches.
	var onN1 int
	f.st.View(func(s *cluster.State) {
		for _, p := range s.Pods {
			if p.OriginNode == "n1" && p.Phase == cluster.PhaseRescheduling {
				onN1++
			}
		}
	})
	if len(ev) != onN1 || onN1 == 0 {
		t.Fatalf("evicted=%d rescheduling-on-n1=%d", len(ev), onN1)
	}

	// Total never drops during the window between eviction and rebuild:
	// Running + Rescheduling == 4 at all times.
	counts, total := f.phaseCount(t)
	if total != 4 {
		t.Fatalf("post-eviction total = %d, want 4 (eviction must not delete)", total)
	}
	if counts[cluster.PhaseRunning]+counts[cluster.PhaseRescheduling] != 4 {
		t.Fatalf("running(%d)+rescheduling(%d) != 4", counts[cluster.PhaseRunning], counts[cluster.PhaseRescheduling])
	}
	t.Logf("right after eviction: running=%d rescheduling=%d total=%d (fully explainable)",
		counts[cluster.PhaseRunning], counts[cluster.PhaseRescheduling], total)

	// Reconcile reschedules what fits onto healthy n2; remainder stays
	// Rescheduling rather than duplicating.
	mustReconcile(t, f.ctl, "web")
	counts, total = f.phaseCount(t)
	if total != 4 {
		t.Fatalf("post-reschedule total = %d, want 4", total)
	}
	t.Logf("after reschedule: running=%d rescheduling=%d total=%d",
		counts[cluster.PhaseRunning], counts[cluster.PhaseRescheduling], total)

	// Repeated failure detection must not double-evict or duplicate. Keep n2
	// heartbeating so only the already-evicted n1 is in question.
	f.clk.advance(100 * time.Second)
	f.st.HeartbeatAt("n2", f.clk.now())
	ev2, err := f.ctl.DetectNodeFailures()
	if err != nil || len(ev2) != 0 {
		t.Fatalf("repeated detection evicted=%v err=%v, want idempotent none", ev2, err)
	}
	_, total = f.phaseCount(t)
	if total != 4 {
		t.Fatalf("repeated detection total = %d, want 4", total)
	}
}

// A node that was evicted after the grace period rejoins scheduling ONLY
// when it verifiably heartbeats again — recovery moves nothing (its pods
// were already migrated), so there is no churn and no duplication.
func TestNodeFailure_EvictedNodeRejoinsOnHeartbeat(t *testing.T) {
	f := newFixture(t, "")
	f.addNode(t, "n1", 8000, 8192)
	f.addNode(t, "n2", 8000, 8192)
	f.addDeployment(t, "web", 4, 1000, 512)
	mustReconcile(t, f.ctl, "web")

	// n1 goes silent past the grace period -> evicted once.
	f.clk.advance(45 * time.Second)
	f.st.HeartbeatAt("n2", f.clk.now())
	ev, err := f.ctl.DetectNodeFailures()
	if err != nil || len(ev) == 0 {
		t.Fatalf("evicted=%v err=%v, want non-empty", ev, err)
	}
	mustReconcile(t, f.ctl, "web")
	counts, total := f.phaseCount(t)
	if total != 4 || counts[cluster.PhaseRunning] != 4 {
		t.Fatalf("counts=%v total=%d, want 4 running after failover", counts, total)
	}
	var evictedFlag bool
	f.st.View(func(s *cluster.State) { evictedFlag = s.Nodes["n1"].Evicted })
	if !evictedFlag {
		t.Fatal("n1 should be marked evicted")
	}

	// While n1 stays silent it must NOT come back, no matter how often we
	// detect or reconcile.
	f.clk.advance(60 * time.Second)
	f.st.HeartbeatAt("n2", f.clk.now())
	if _, err := f.ctl.DetectNodeFailures(); err != nil {
		t.Fatal(err)
	}
	f.st.View(func(s *cluster.State) {
		if s.Nodes["n1"].Ready {
			t.Fatal("silent evicted node must not become Ready again")
		}
	})

	// n1 verifiably reports again -> the next detection pass makes it Ready
	// and clears the eviction mark; nothing is migrated back (anti-flap).
	f.st.HeartbeatAt("n1", f.clk.now())
	ev, err = f.ctl.DetectNodeFailures()
	if err != nil || len(ev) != 0 {
		t.Fatalf("recovery pass evicted=%v err=%v, want none", ev, err)
	}
	f.st.View(func(s *cluster.State) {
		n1 := s.Nodes["n1"]
		if !n1.Ready || n1.Evicted {
			t.Fatalf("recovered node: ready=%t evicted=%t, want ready/not-evicted", n1.Ready, n1.Evicted)
		}
	})
	res := mustReconcile(t, f.ctl, "web")
	if len(res.Bound) != 0 || len(res.Created) != 0 {
		t.Fatalf("recovery reconcile churn: bound=%v created=%v, want none", res.Bound, res.Created)
	}

	// New replicas may now land on the rejoined node (it has 4000m free).
	if err := f.st.ScaleDeployment("web", 6); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, f.ctl, "web")
	var onN1 int
	f.st.View(func(s *cluster.State) {
		for _, p := range s.Pods {
			if p.NodeName == "n1" && p.Phase == cluster.PhaseRunning {
				onN1++
			}
		}
	})
	if onN1 != 2 {
		t.Fatalf("pods on rejoined n1 = %d, want 2 new replicas", onN1)
	}
	_, total = f.phaseCount(t)
	if total != 6 {
		t.Fatalf("total = %d, want 6", total)
	}
	t.Logf("evicted node rejoined only after real heartbeat; zero churn, total stable")
}
