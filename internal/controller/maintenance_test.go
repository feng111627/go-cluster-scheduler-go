package controller

import (
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
// nodes INSIDE the same operation — no extra reconcile trigger is needed —
// and the instance total stays explainable throughout.
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

	dr, err := f.ctl.EnterMaintenance("n1")
	if err != nil {
		t.Fatalf("enter maintenance: %v", err)
	}
	t.Logf("drain result: drained=%v migrated=%v waiting=%v", dr.Drained, dr.Migrated, dr.Waiting)
	if len(dr.Drained) != 2 {
		t.Fatalf("drained %d pods, want 2", len(dr.Drained))
	}

	// The migration already happened inside EnterMaintenance: all 4 pods are
	// Running again, the 2 drained ones now on healthy n2 — without any
	// additional reconcile being triggered.
	if len(dr.Migrated) != 2 || len(dr.Waiting) != 0 {
		t.Fatalf("migrated=%v waiting=%v, want 2 migrated and none waiting", dr.Migrated, dr.Waiting)
	}
	counts, total := f.phaseCount(t)
	if total != 4 || counts[cluster.PhaseRunning] != 4 {
		t.Fatalf("post-drain counts=%v total=%d, want 4 running (migration is immediate)", counts, total)
	}
	if got := podsOnNode(f, "n2"); len(got) != 4 {
		t.Fatalf("n2 pods = %v, want all 4 migrated there", got)
	}
	if got := podsOnNode(f, "n1"); len(got) != 0 {
		t.Fatalf("maintenance node n1 still runs %v, want none", got)
	}

	// A follow-up reconcile is a no-op: nothing is left waiting.
	res := mustReconcile(t, f.ctl, "web")
	if len(res.Bound) != 0 || len(res.Created) != 0 {
		t.Fatalf("reconcile after drain bound=%v created=%v, want zero (migration already done)", res.Bound, res.Created)
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
	if err != nil || len(again.Drained) != 0 {
		t.Fatalf("re-enter drained=%v err=%v, want idempotent empty", again.Drained, err)
	}
}

// podWaitReason returns the persisted wait reason of one pod.
func podWaitReason(f *fixture, uid string) cluster.WaitReason {
	var r cluster.WaitReason
	f.st.View(func(s *cluster.State) { r = s.Pods[uid].WaitReason })
	return r
}

// When healthy nodes cannot fit the drained pods, they wait in Rescheduling
// with a distinguishable, persisted reason (MaintenanceDrain, not NodeLost) —
// never dropped, never forced onto the maintenance node.
func TestMaintenance_InsufficientCapacityWaitsWithReason(t *testing.T) {
	f := newFixture(t, "")
	f.addNode(t, "n1", 4000, 4096)
	f.addNode(t, "n2", 2000, 4096) // fits only 2 pods of 1000m
	f.addDeployment(t, "web", 3, 1000, 512)
	mustReconcile(t, f.ctl, "web")
	// Placement: web-0->n1, web-1->n2, web-2->n1 (n1 has more remaining CPU).

	dr, err := f.ctl.EnterMaintenance("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(dr.Drained) != 2 {
		t.Fatalf("drained %d, want 2", len(dr.Drained))
	}

	// n2 has 1000m free: exactly one drained pod migrates immediately, the
	// other must wait — with its reason reported by the drain result itself.
	t.Logf("drain result: drained=%v migrated=%v waiting=%v", dr.Drained, dr.Migrated, dr.Waiting)
	if len(dr.Migrated) != 1 || len(dr.Waiting) != 1 || len(dr.WaitErrors) != 1 {
		t.Fatalf("migrated=%v waiting=%v waitErrors=%d, want 1/1/1", dr.Migrated, dr.Waiting, len(dr.WaitErrors))
	}
	se := dr.WaitErrors[0]
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

	// The waiting pod still exists, carries the MAINTENANCE wait reason (not
	// the node-failure one), is not on the maintenance node, and the total is
	// still exactly the desired count.
	waitUID := dr.Waiting[0]
	if got := podWaitReason(f, waitUID); got != cluster.WaitReasonMaintenanceDrain {
		t.Fatalf("waiting pod reason = %q, want %q (distinguishable from %q)",
			got, cluster.WaitReasonMaintenanceDrain, cluster.WaitReasonNodeLost)
	}
	counts, total := f.phaseCount(t)
	if total != 3 || counts[cluster.PhaseRescheduling] != 1 || counts[cluster.PhaseRunning] != 2 {
		t.Fatalf("counts=%v total=%d, want 2 running + 1 rescheduling = 3", counts, total)
	}
	if got := podsOnNode(f, "n1"); len(got) != 0 {
		t.Fatalf("waiting pod must not be forced onto maintenance node, got %v", got)
	}

	// Once capacity appears (new node), a reconcile places the waiting pod
	// and clears its wait reason.
	f.addNode(t, "n3", 4000, 4096)
	mustReconcile(t, f.ctl, "web")
	counts, total = f.phaseCount(t)
	if total != 3 || counts[cluster.PhaseRunning] != 3 {
		t.Fatalf("after adding capacity counts=%v total=%d, want 3 running", counts, total)
	}
	if got := podWaitReason(f, waitUID); got != cluster.WaitReasonNone {
		t.Fatalf("bound pod wait reason = %q, want cleared", got)
	}
}

// The maintenance flag, the drain progress AND the per-pod wait reason
// survive a process restart: a pod still waiting for capacity is recognized
// as maintenance-drained (not failure-evicted) by the new process.
func TestMaintenance_FlagPersistedAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "state.json")

	var waitUID string
	func() {
		f := newFixture(t, file)
		f.addNode(t, "n1", 4000, 4096)
		f.addNode(t, "n2", 2000, 4096) // fits only 2 pods of 1000m
		f.addDeployment(t, "web", 3, 1000, 512)
		mustReconcile(t, f.ctl, "web")
		dr, err := f.ctl.EnterMaintenance("n1")
		if err != nil {
			t.Fatal(err)
		}
		// One pod migrated onto n2, one is left waiting for capacity.
		if len(dr.Migrated) != 1 || len(dr.Waiting) != 1 {
			t.Fatalf("pre-restart migrated=%v waiting=%v, want 1/1", dr.Migrated, dr.Waiting)
		}
		waitUID = dr.Waiting[0]
		// Crash/restart BEFORE anything else happens.
	}()

	// Fresh process: flag, drain progress and wait reason must all survive.
	f2 := newFixture(t, file)
	if !nodeMaintenance(f2, "n1") {
		t.Fatal("maintenance flag lost across restart")
	}
	counts, total := f2.phaseCount(t)
	if total != 3 || counts[cluster.PhaseRescheduling] != 1 || counts[cluster.PhaseRunning] != 2 {
		t.Fatalf("after restart counts=%v total=%d, want 2 running + 1 rescheduling (progress kept)", counts, total)
	}
	if got := podWaitReason(f2, waitUID); got != cluster.WaitReasonMaintenanceDrain {
		t.Fatalf("after restart wait reason = %q, want %q (still distinguishable from node failure)",
			got, cluster.WaitReasonMaintenanceDrain)
	}

	// Re-entering maintenance after restart is idempotent: nothing to drain,
	// nothing re-migrated — progress continues, it does not restart.
	again, err := f2.ctl.EnterMaintenance("n1")
	if err != nil || len(again.Drained) != 0 || len(again.Migrated) != 0 {
		t.Fatalf("re-enter after restart = %+v err=%v, want empty (no duplicate migration)", again, err)
	}

	// Once capacity appears, the waiting pod is placed onto a healthy node —
	// never back onto the maintenance node.
	f2.addNode(t, "n3", 4000, 4096)
	mustReconcile(t, f2.ctl, "web")
	if got := podsOnNode(f2, "n1"); len(got) != 0 {
		t.Fatalf("after restart n1 pods = %v, want 0", got)
	}
	counts, _ = f2.phaseCount(t)
	if counts[cluster.PhaseRunning] != 3 {
		t.Fatalf("after restart+capacity running = %d, want 3", counts[cluster.PhaseRunning])
	}
	if got := podWaitReason(f2, waitUID); got != cluster.WaitReasonNone {
		t.Fatalf("bound pod wait reason = %q, want cleared", got)
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

// Exiting maintenance on a node that has NOT actually recovered (still
// silent, no fresh heartbeat) must not treat it as healthy: it stays
// NotReady, nothing is placed on it, and the failure detector does not
// ping-pong it. Only a real heartbeat makes it schedulable again.
func TestMaintenance_ExitWhileSilentStaysUnschedulable(t *testing.T) {
	f := newFixture(t, "")
	f.addNode(t, "n1", 8000, 8192)
	f.addNode(t, "n2", 8000, 8192)
	f.addDeployment(t, "web", 4, 1000, 512)
	mustReconcile(t, f.ctl, "web")

	dr, err := f.ctl.EnterMaintenance("n1")
	if err != nil || len(dr.Migrated) != 2 {
		t.Fatalf("enter: %+v err=%v, want 2 migrated", dr, err)
	}
	if got := podsOnNode(f, "n2"); len(got) != 4 {
		t.Fatalf("n2 pods = %v, want all 4 after drain", got)
	}

	// The node stays silent well beyond the heartbeat timeout while the
	// operator ends maintenance (e.g. the machine is still being repaired).
	f.clk.advance(60 * time.Second)
	f.st.HeartbeatAt("n2", f.clk.now()) // n2 stays healthy
	if err := f.ctl.ExitMaintenance("n1"); err != nil {
		t.Fatal(err)
	}
	if nodeMaintenance(f, "n1") {
		t.Fatal("maintenance flag should be cleared")
	}

	// The silent node must NOT be a scheduling candidate.
	var ready bool
	f.st.View(func(s *cluster.State) { ready = s.Nodes["n1"].Ready })
	if ready {
		t.Fatal("silent node must not become Ready on exit (would cause place-then-evict flap)")
	}

	// Scale up: new pods must go to healthy n2, never to the silent n1.
	if err := f.st.ScaleDeployment("web", 6); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, f.ctl, "web")
	if got := podsOnNode(f, "n1"); len(got) != 0 {
		t.Fatalf("pods placed on still-silent node: %v", got)
	}
	if got := podsOnNode(f, "n2"); len(got) != 6 {
		t.Fatalf("n2 pods = %v, want 6", got)
	}

	// Failure detection right after exit must not evict anything (the node
	// got a fresh grace window from the exit, and it runs no pods anyway).
	ev, err := f.ctl.DetectNodeFailures()
	if err != nil || len(ev) != 0 {
		t.Fatalf("detection after exit evicted=%v err=%v, want none", ev, err)
	}

	// The node actually recovers: it heartbeats again. The next detection
	// pass flips it Ready — only NOW does it participate in scheduling.
	f.st.HeartbeatAt("n1", f.clk.now())
	if _, err := f.ctl.DetectNodeFailures(); err != nil {
		t.Fatal(err)
	}
	f.st.View(func(s *cluster.State) {
		if !s.Nodes["n1"].Ready {
			t.Fatal("node that verifiably re-reported must become Ready")
		}
	})

	// And now new placements may land on it (n1 has 4000m free, n2 is full).
	if err := f.st.ScaleDeployment("web", 8); err != nil {
		t.Fatal(err)
	}
	res := mustReconcile(t, f.ctl, "web")
	if len(res.Bound) != 2 {
		t.Fatalf("bound = %v, want 2", res.Bound)
	}
	if got := podsOnNode(f, "n1"); len(got) != 2 {
		t.Fatalf("n1 pods = %v, want the 2 new replicas after real recovery", got)
	}
	_, total := f.phaseCount(t)
	if total != 8 {
		t.Fatalf("total = %d, want 8", total)
	}
	t.Logf("silent-exit scenario consistent: no placement on silent node, rejoin only after real heartbeat")
}

// Maintenance-drain waits and failure-eviction waits coexist and stay
// distinguishable per pod, in memory and after a process restart.
func TestMaintenance_WaitReasonDistinctFromFailure(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "state.json")
	var maintUID, lostUID string

	func() {
		f := newFixture(t, file)
		// n1 (maintenance candidate), n2 (will be lost), n3 small healthy.
		f.addNode(t, "n1", 2000, 4096)
		f.addNode(t, "n2", 2000, 4096)
		f.addNode(t, "n3", 1000, 4096) // fits exactly 1 pod of 1000m
		f.addDeployment(t, "web", 4, 1000, 512)
		mustReconcile(t, f.ctl, "web")

		// Drain n1: 2 pods leave, n3 has 1000m free -> 1 migrates, 1 waits.
		dr, err := f.ctl.EnterMaintenance("n1")
		if err != nil {
			t.Fatal(err)
		}
		if len(dr.Waiting) != 1 {
			t.Fatalf("waiting = %v, want exactly 1 maintenance waiter", dr.Waiting)
		}
		maintUID = dr.Waiting[0]

		// Lose n2 beyond the eviction grace: its 2 pods are evicted, n3 is
		// now full -> both wait with the NODE-FAILURE reason.
		f.clk.advance(45 * time.Second)
		f.st.HeartbeatAt("n3", f.clk.now())
		ev, err := f.ctl.DetectNodeFailures()
		if err != nil || len(ev) != 2 {
			t.Fatalf("evicted = %v err=%v, want 2", ev, err)
		}
		mustReconcile(t, f.ctl, "web") // try to place; nothing fits
		// Find one still-waiting failure-evicted pod.
		f.st.View(func(s *cluster.State) {
			for uid, p := range s.Pods {
				if p.Phase == cluster.PhaseRescheduling && p.WaitReason == cluster.WaitReasonNodeLost {
					lostUID = uid
				}
			}
		})
		if lostUID == "" {
			t.Fatal("expected at least one failure-evicted pod still waiting")
		}
		// Restart with both kinds of waiters persisted.
	}()

	// After a restart the two wait causes are still distinguishable.
	f2 := newFixture(t, file)
	if got := podWaitReason(f2, maintUID); got != cluster.WaitReasonMaintenanceDrain {
		t.Fatalf("maintenance waiter reason = %q, want %q", got, cluster.WaitReasonMaintenanceDrain)
	}
	if got := podWaitReason(f2, lostUID); got != cluster.WaitReasonNodeLost {
		t.Fatalf("failure waiter reason = %q, want %q", got, cluster.WaitReasonNodeLost)
	}
	counts, total := f2.phaseCount(t)
	if total != 4 {
		t.Fatalf("total = %d, want 4 (no instance lost across restart)", total)
	}
	t.Logf("after restart: %s waits due to %s, %s waits due to %s (counts=%v)",
		maintUID, cluster.WaitReasonMaintenanceDrain, lostUID, cluster.WaitReasonNodeLost, counts)

	// New capacity absorbs both waiters; reasons clear on binding.
	f2.addNode(t, "n4", 8000, 8192)
	mustReconcile(t, f2.ctl, "web")
	counts, _ = f2.phaseCount(t)
	if counts[cluster.PhaseRunning] != 4 {
		t.Fatalf("running = %d, want 4 after capacity added", counts[cluster.PhaseRunning])
	}
	for _, uid := range []string{maintUID, lostUID} {
		if got := podWaitReason(f2, uid); got != cluster.WaitReasonNone {
			t.Fatalf("pod %s bound but wait reason = %q, want cleared", uid, got)
		}
	}
}

// Concurrent maintenance/recovery/reconcile storm with a process restart in
// the middle: the persisted state stays consistent, drain progress continues
// (never restarts, never duplicates) and the instance total never drifts.
func TestMaintenance_ConcurrentStormWithRestart(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "state.json")

	func() {
		f := newFixture(t, file)
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
		// Restart right after the storm, in whatever state was persisted.
	}()

	// Fresh process on the persisted state: invariants must hold.
	f2 := newFixture(t, file)
	_, total := f2.phaseCount(t)
	if total != 8 {
		t.Fatalf("after restart total = %d, want exactly 8 (no loss, no duplication)", total)
	}

	// Drive to a deterministic end state: maintenance on -> fully drained and
	// migrated (idempotent even if the storm left the flag already set).
	dr, err := f2.ctl.EnterMaintenance("n1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("post-restart drain: %+v", dr)
	if got := podsOnNode(f2, "n1"); len(got) != 0 {
		t.Fatalf("n1 pods = %v, want 0 after drain", got)
	}
	counts, total := f2.phaseCount(t)
	if total != 8 || counts[cluster.PhaseRunning] != 8 {
		t.Fatalf("counts=%v total=%d, want 8 running on healthy nodes", counts, total)
	}
	if got := podsOnNode(f2, "n2"); len(got) != 8 {
		t.Fatalf("n2 pods = %v, want all 8", got)
	}

	// Recovery (node is heartbeating: fixture clock never advanced) and one
	// more restart: state must remain stable with zero churn.
	if err := f2.ctl.ExitMaintenance("n1"); err != nil {
		t.Fatal(err)
	}
	f3 := newFixture(t, file)
	res := mustReconcile(t, f3.ctl, "web")
	if len(res.Bound) != 0 || len(res.Created) != 0 {
		t.Fatalf("post-restart reconcile churn: bound=%v created=%v, want none", res.Bound, res.Created)
	}
	_, total = f3.phaseCount(t)
	if total != 8 {
		t.Fatalf("final total = %d, want 8", total)
	}
	t.Logf("concurrent storm + 2 restarts: total stable at 8, no duplicate migration")
}
