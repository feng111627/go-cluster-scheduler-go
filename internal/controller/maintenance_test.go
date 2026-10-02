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
)

// podsOnNode returns the UIDs of Running pods bound to nodeName.
func podsOnNode(f *fixture, nodeName string) []string {
	var out []string
	f.st.View(func(s *cluster.State) {
		for uid, p := range s.Pods {
			if p.NodeName == nodeName && p.Phase == cluster.PhaseRunning {
				out = append(out, uid)
			}
		}
	})
	return out
}

func nodeMaintenance(f *fixture, nodeName string) bool {
	var m bool
	f.st.View(func(s *cluster.State) { m = s.Nodes[nodeName].Maintenance })
	return m
}

// Planned maintenance drains the node and migrates its pods onto healthy
// nodes deterministically; the instance total stays explainable throughout.
func TestMaintenance_DrainMigratesToHealthyNodes(t *testing.T) {
	f := newFixture(t, "")
	f.addNode(t, "n1", 4000, 4096)
	f.addNode(t, "n2", 4000, 4096)
	f.addDeployment(t, "web", 4, 1000, 512)
	mustReconcile(t, f.ctl, "web")

	// Deterministic pre-drain placement: web-0/web-2 on n1, web-1/web-3 on n2.
	if got := podsOnNode(f, "n1"); len(got) != 2 {
		t.Fatalf("pre-drain n1 pods = %v, want 2", got)
	}

	drained, err := f.ctl.EnterMaintenance("n1")
	if err != nil {
		t.Fatalf("enter maintenance: %v", err)
	}
	t.Logf("drained from n1: %v", drained)
	if len(drained) != 2 {
		t.Fatalf("drained %d pods, want 2", len(drained))
	}

	// Between drain and reschedule: total still 4, nothing left running on n1.
	counts, total := f.phaseCount(t)
	if total != 4 || counts[cluster.PhaseRescheduling] != 2 {
		t.Fatalf("post-drain counts=%v total=%d, want 2 rescheduling of 4", counts, total)
	}
	if got := podsOnNode(f, "n1"); len(got) != 0 {
		t.Fatalf("maintenance node n1 still runs %v, want none", got)
	}

	// Reconcile migrates them onto n2 (the only feasible node) — deterministic.
	res := mustReconcile(t, f.ctl, "web")
	if len(res.Bound) != 2 {
		t.Fatalf("rebound %d pods, want 2", len(res.Bound))
	}
	counts, total = f.phaseCount(t)
	if total != 4 || counts[cluster.PhaseRunning] != 4 {
		t.Fatalf("post-migration counts=%v total=%d, want 4 running", counts, total)
	}
	if got := podsOnNode(f, "n2"); len(got) != 4 {
		t.Fatalf("n2 pods = %v, want all 4 migrated there", got)
	}
	if got := podsOnNode(f, "n1"); len(got) != 0 {
		t.Fatalf("n1 pods = %v, want 0 (maintenance node must stay drained)", got)
	}

	// New replicas must not land on the maintenance node either.
	if err := f.st.ScaleDeployment("web", 5); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, f.ctl, "web")
	if got := podsOnNode(f, "n1"); len(got) != 0 {
		t.Fatalf("after scale-up n1 pods = %v, want 0 (NodeMaintenance rejection)", got)
	}
	_, total = f.phaseCount(t)
	if total != 5 {
		t.Fatalf("total = %d, want 5", total)
	}

	// Re-entering maintenance is idempotent: nothing left to drain.
	again, err := f.ctl.EnterMaintenance("n1")
	if err != nil || len(again) != 0 {
		t.Fatalf("re-enter drained=%v err=%v, want idempotent empty", again, err)
	}
}

// When healthy nodes cannot fit the drained pods, they wait in Rescheduling
// with a distinguishable reason — never dropped, never forced onto the
// maintenance node.
func TestMaintenance_InsufficientCapacityWaitsWithReason(t *testing.T) {
	f := newFixture(t, "")
	f.addNode(t, "n1", 4000, 4096)
	f.addNode(t, "n2", 2000, 4096) // fits only 2 pods of 1000m
	f.addDeployment(t, "web", 3, 1000, 512)
	mustReconcile(t, f.ctl, "web")
	// Placement: web-0->n1, web-1->n2, web-2->n1 (n1 has more remaining CPU).

	drained, err := f.ctl.EnterMaintenance("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(drained) != 2 {
		t.Fatalf("drained %d, want 2", len(drained))
	}

	// n2 has 1000m free: exactly one drained pod fits, the other must wait.
	_, err = f.ctl.ReconcileDeployment("web")
	var rerr *ReconcileError
	if !errors.As(err, &rerr) {
		t.Fatalf("want ReconcileError for the pod that does not fit, got %v", err)
	}
	if len(rerr.ScheduleErrors) != 1 {
		t.Fatalf("unschedulable = %d, want 1", len(rerr.ScheduleErrors))
	}
	se := rerr.ScheduleErrors[0]
	t.Logf("waiting pod %s aggregate reason: %s (per-node: %v)", se.PodUID, se.AggregateReason(), se.Failures)
	if se.AggregateReason() != scheduler.ReasonInsufficientCPU {
		t.Fatalf("aggregate = %s, want InsufficientCPU (distinguishable from maintenance itself)", se.AggregateReason())
	}
	// The per-node reasons must show WHY: n1 under maintenance, n2 full.
	var sawMaint, sawCPU bool
	for _, nf := range se.Failures {
		sawMaint = sawMaint || nf.Reason == scheduler.ReasonNodeMaintenance
		sawCPU = sawCPU || nf.Reason == scheduler.ReasonInsufficientCPU
	}
	if !sawMaint || !sawCPU {
		t.Fatalf("per-node failures = %v, want NodeMaintenance(n1) + InsufficientCPU(n2)", se.Failures)
	}

	// The waiting pod still exists, is not on the maintenance node, and the
	// total is still exactly the desired count.
	counts, total := f.phaseCount(t)
	if total != 3 || counts[cluster.PhaseRescheduling] != 1 || counts[cluster.PhaseRunning] != 2 {
		t.Fatalf("counts=%v total=%d, want 2 running + 1 rescheduling = 3", counts, total)
	}
	if got := podsOnNode(f, "n1"); len(got) != 0 {
		t.Fatalf("waiting pod must not be forced onto maintenance node, got %v", got)
	}

	// Once n2 grows (simulated by adding a node), the waiting pod is placed.
	f.addNode(t, "n3", 4000, 4096)
	mustReconcile(t, f.ctl, "web")
	counts, total = f.phaseCount(t)
	if total != 3 || counts[cluster.PhaseRunning] != 3 {
		t.Fatalf("after adding capacity counts=%v total=%d, want 3 running", counts, total)
	}
}

// The maintenance flag and drain progress survive a process restart.
func TestMaintenance_FlagPersistedAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "state.json")

	func() {
		f := newFixture(t, file)
		f.addNode(t, "n1", 4000, 4096)
		f.addNode(t, "n2", 4000, 4096)
		f.addDeployment(t, "web", 2, 1000, 512)
		mustReconcile(t, f.ctl, "web")
		if _, err := f.ctl.EnterMaintenance("n1"); err != nil {
			t.Fatal(err)
		}
		// Crash/restart BEFORE the rescheduling reconcile happens.
	}()

	// Fresh process: flag must still be set and drain progress must continue.
	f2 := newFixture(t, file)
	if !nodeMaintenance(f2, "n1") {
		t.Fatal("maintenance flag lost across restart")
	}
	counts, total := f2.phaseCount(t)
	if total != 2 || counts[cluster.PhaseRescheduling] != 1 {
		t.Fatalf("after restart counts=%v total=%d, want 1 rescheduling of 2 (progress kept)", counts, total)
	}

	// Reconcile places the waiting pod onto healthy n2, never back onto n1.
	mustReconcile(t, f2.ctl, "web")
	if got := podsOnNode(f2, "n1"); len(got) != 0 {
		t.Fatalf("after restart n1 pods = %v, want 0", got)
	}
	counts, _ = f2.phaseCount(t)
	if counts[cluster.PhaseRunning] != 2 {
		t.Fatalf("after restart+reconcile running = %d, want 2", counts[cluster.PhaseRunning])
	}

	// Re-entering maintenance after restart is idempotent (nothing to drain).
	again, err := f2.ctl.EnterMaintenance("n1")
	if err != nil || len(again) != 0 {
		t.Fatalf("re-enter after restart drained=%v err=%v, want empty", again, err)
	}
}

// After maintenance ends the node rejoins scheduling, but running pods are
// never migrated back (anti-flap); only new/unbound pods may land on it, and
// the failure detector does not pounce on the stale heartbeat history.
func TestMaintenance_ExitRejoinsWithoutFlap(t *testing.T) {
	f := newFixture(t, "")
	f.addNode(t, "n1", 4000, 4096)
	f.addNode(t, "n2", 4000, 4096)
	f.addDeployment(t, "web", 4, 1000, 512)
	mustReconcile(t, f.ctl, "web")

	if _, err := f.ctl.EnterMaintenance("n1"); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, f.ctl, "web")
	if got := podsOnNode(f, "n2"); len(got) != 4 {
		t.Fatalf("n2 pods = %v, want all 4 after drain", got)
	}

	// Node comes back.
	if err := f.ctl.ExitMaintenance("n1"); err != nil {
		t.Fatal(err)
	}
	if nodeMaintenance(f, "n1") {
		t.Fatal("maintenance flag should be cleared")
	}

	// Anti-flap: reconcile must NOT move anything back onto n1.
	res := mustReconcile(t, f.ctl, "web")
	if len(res.Bound) != 0 || len(res.Created) != 0 {
		t.Fatalf("reconcile after exit bound=%v created=%v, want zero churn", res.Bound, res.Created)
	}
	if got := podsOnNode(f, "n1"); len(got) != 0 {
		t.Fatalf("pods moved back to n1: %v — recovery must not migrate running pods", got)
	}

	// The failure detector must not evict the just-recovered node even though
	// its heartbeat history is old: ExitMaintenance refreshed LastSeen.
	f.clk.advance(5 * time.Second) // within heartbeat timeout of the fresh LastSeen
	ev, err := f.ctl.DetectNodeFailures()
	if err != nil || len(ev) != 0 {
		t.Fatalf("detection right after exit evicted=%v err=%v, want none", ev, err)
	}

	// The rejoined node receives NEW placements deterministically: scale up
	// and the new pods must land on n1 (it has all 4000m free, n2 has none).
	if err := f.st.ScaleDeployment("web", 6); err != nil {
		t.Fatal(err)
	}
	res = mustReconcile(t, f.ctl, "web")
	if len(res.Bound) != 2 {
		t.Fatalf("scale-up bound = %v, want 2", res.Bound)
	}
	if got := podsOnNode(f, "n1"); len(got) != 2 {
		t.Fatalf("n1 pods = %v, want the 2 new replicas (deterministic rejoin)", got)
	}
	_, total := f.phaseCount(t)
	if total != 6 {
		t.Fatalf("total = %d, want 6", total)
	}

	// Exiting again is a no-op; repeated reconciles stay stable (no flap).
	if err := f.ctl.ExitMaintenance("n1"); err != nil {
		t.Fatal(err)
	}
	res = mustReconcile(t, f.ctl, "web")
	if len(res.Bound) != 0 || len(res.Created) != 0 {
		t.Fatalf("post-exit reconcile churn: bound=%v created=%v, want none", res.Bound, res.Created)
	}
}

// Maintenance nodes are deliberately out of service: the failure detector
// must skip them (no Unreachable flapping, no eviction double-handling).
func TestMaintenance_SkippedByFailureDetector(t *testing.T) {
	f := newFixture(t, "")
	f.addNode(t, "n1", 4000, 4096)
	f.addNode(t, "n2", 4000, 4096)
	f.addDeployment(t, "web", 2, 1000, 512)
	mustReconcile(t, f.ctl, "web")

	if _, err := f.ctl.EnterMaintenance("n1"); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, f.ctl, "web")

	// n1 stays silent far beyond the eviction grace; n2 keeps heartbeating.
	f.clk.advance(120 * time.Second)
	f.st.HeartbeatAt("n2", f.clk.now())
	ev, err := f.ctl.DetectNodeFailures()
	if err != nil || len(ev) != 0 {
		t.Fatalf("detection evicted=%v err=%v, want none (maintenance node skipped)", ev, err)
	}
	f.st.View(func(s *cluster.State) {
		n1 := s.Nodes["n1"]
		if n1.Evicted {
			t.Fatal("maintenance node must not be marked evicted by failure detector")
		}
		if !n1.Maintenance {
			t.Fatal("maintenance flag must survive failure detection")
		}
	})
	counts, total := f.phaseCount(t)
	if total != 2 || counts[cluster.PhaseRunning] != 2 {
		t.Fatalf("counts=%v total=%d, want 2 running (no detector churn)", counts, total)
	}
}

// Concurrent enter/exit/reconcile/detect must stay consistent: the store
// serializes everything, the instance total never drifts, and after the
// storm a final drain+reconcile reaches the expected end state.
func TestMaintenance_ConcurrentEnterExitConsistent(t *testing.T) {
	f := newFixture(t, "")
	f.addNode(t, "n1", 8000, 8192)
	f.addNode(t, "n2", 8000, 8192)
	f.addDeployment(t, "web", 8, 500, 256)
	mustReconcile(t, f.ctl, "web")

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(3)
		go func() { defer wg.Done(); _, _ = f.ctl.EnterMaintenance("n1") }()
		go func() { defer wg.Done(); _ = f.ctl.ExitMaintenance("n1") }()
		go func() { defer wg.Done(); _ = mustReconcile(t, f.ctl, "web") }()
	}
	wg.Wait()

	// Invariant at any quiescent point: exactly the desired number of
	// non-terminal pods exists, and none is ever duplicated.
	_, total := f.phaseCount(t)
	if total != 8 {
		t.Fatalf("after concurrent storm total = %d, want exactly 8", total)
	}

	// Drive to a deterministic end state: maintenance on -> fully drained.
	if _, err := f.ctl.EnterMaintenance("n1"); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, f.ctl, "web")
	if got := podsOnNode(f, "n1"); len(got) != 0 {
		t.Fatalf("final drain: n1 pods = %v, want 0", got)
	}
	counts, total := f.phaseCount(t)
	if total != 8 || counts[cluster.PhaseRunning] != 8 {
		t.Fatalf("final counts=%v total=%d, want 8 running", counts, total)
	}

	// And back out: recovery with zero churn.
	if err := f.ctl.ExitMaintenance("n1"); err != nil {
		t.Fatal(err)
	}
	res := mustReconcile(t, f.ctl, "web")
	if len(res.Bound) != 0 || len(res.Created) != 0 {
		t.Fatalf("post-storm exit churn: bound=%v created=%v, want none", res.Bound, res.Created)
	}
	_, total = f.phaseCount(t)
	if total != 8 {
		t.Fatalf("final total = %d, want 8", total)
	}
	t.Logf("concurrent maintenance storm finished consistently: %s",
		fmt.Sprintf("total=%d running=%d", total, counts[cluster.PhaseRunning]))
}
