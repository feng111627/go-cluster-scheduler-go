package controller

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/feng111627/go-cluster-scheduler-go/internal/cluster"
)

// assertInvariants checks the cluster-wide accounting invariants that must
// hold at EVERY instant (including mid-drain):
//   - non-terminal pod count equals wantTotal (no instance appears/vanishes);
//   - node Used equals the sum of Running pods bound to it (no phantom usage);
//   - no Running pod sits on a cordoned node;
//   - Running pods carry no wait reason, waiting pods do carry one.
func assertInvariants(t *testing.T, f *fixture, wantTotal int) {
	t.Helper()
	f.st.View(func(s *cluster.State) {
		usedByNode := map[string]cluster.Resources{}
		total := 0
		for _, p := range s.Pods {
			if p.Phase == cluster.PhaseFailed {
				continue
			}
			total++
			switch p.Phase {
			case cluster.PhaseRunning:
				if p.NodeName == "" {
					t.Errorf("running pod %s has empty node", p.UID)
				}
				if p.WaitReason != "" {
					t.Errorf("running pod %s still carries waitReason=%s", p.UID, p.WaitReason)
				}
				n := s.Nodes[p.NodeName]
				if n == nil {
					t.Errorf("pod %s bound to missing node %s", p.UID, p.NodeName)
					continue
				}
				if n.UnderMaintenance {
					t.Errorf("pod %s still Running on cordoned node %s", p.UID, p.NodeName)
				}
				u := usedByNode[p.NodeName]
				usedByNode[p.NodeName] = u.Add(p.Requests)
			case cluster.PhasePending, cluster.PhaseRescheduling:
				if p.NodeName != "" {
					t.Errorf("unbound pod %s (%s) still references node %s", p.UID, p.Phase, p.NodeName)
				}
				if p.Phase == cluster.PhaseRescheduling && p.WaitReason == "" {
					t.Errorf("rescheduling pod %s has no distinguishable wait reason", p.UID)
				}
			}
		}
		if total != wantTotal {
			t.Errorf("instance total = %d, want %d", total, wantTotal)
		}
		for name, n := range s.Nodes {
			got := usedByNode[name]
			if got != n.Used {
				t.Errorf("node %s used=%v but running pods sum=%v", name, n.Used, got)
			}
		}
	})
}

func placementSnapshot(t *testing.T, f *fixture) map[string]string {
	t.Helper()
	m := map[string]string{}
	f.st.View(func(s *cluster.State) {
		for uid, p := range s.Pods {
			m[uid] = p.NodeName
		}
	})
	return m
}

// TestMaintenance_DrainsAndMigrates: a planned maintenance call cordons the
// node, persists the flag, and migrates every Running pod off it
// deterministically; the instance total and per-node accounting stay exact.
func TestMaintenance_DrainsAndMigrates(t *testing.T) {
	f := newFixture(t, "")
	f.addNode(t, "n1", 4000, 4096)
	f.addNode(t, "n2", 4000, 4096)
	f.addDeployment(t, "web", 4, 1000, 512)
	mustReconcile(t, f.ctl, "web") // deterministic: 0,2 on n1 ; 1,3 on n2

	res, err := f.ctl.StartNodeMaintenance("n1")
	if err != nil {
		t.Fatalf("start maintenance: %v", err)
	}
	if res.FlagAlreadySet {
		t.Fatal("first drain should report flag transition")
	}
	if len(res.Migrated) != 2 || len(res.Waiting) != 0 || len(res.StillRunning) != 0 {
		t.Fatalf("drain result = %+v, want 2 migrated / 0 waiting / 0 left", res)
	}
	// Deterministic targets: both n1 pods must land on the only healthy node.
	want := map[string]string{
		"default-web-0": "n2",
		"default-web-2": "n2",
	}
	for uid, node := range want {
		f.st.View(func(s *cluster.State) {
			if got := s.Pods[uid].NodeName; got != node {
				t.Errorf("pod %s on %s, want %s", uid, got, node)
			}
		})
	}
	assertInvariants(t, f, 4)

	// The flag is durably set in state and the node's reservation is released.
	f.st.View(func(s *cluster.State) {
		if !s.Nodes["n1"].UnderMaintenance {
			t.Error("UnderMaintenance flag not persisted on node")
		}
		if s.Nodes["n1"].Used.CPU != 0 || s.Nodes["n2"].Used.CPU != 4000 {
			t.Errorf("used after drain: n1=%v n2=%v", s.Nodes["n1"].Used, s.Nodes["n2"].Used)
		}
	})

	// A second drain call resumes without moving anything again.
	res2, err := f.ctl.StartNodeMaintenance("n1")
	if err != nil {
		t.Fatalf("repeat drain: %v", err)
	}
	if !res2.FlagAlreadySet || len(res2.Migrated) != 0 || len(res2.Waiting) != 0 {
		t.Fatalf("repeat drain = %+v, want a pure no-op resume", res2)
	}
	assertInvariants(t, f, 4)

	// Reconcile while cordoned must not place anything back onto n1.
	mustReconcile(t, f.ctl, "web")
	assertInvariants(t, f, 4)
}

// TestMaintenance_DeterministicTargets: identical inputs rebuilt from scratch
// must yield the identical pod->node mapping after the drain.
func TestMaintenance_DeterministicTargets(t *testing.T) {
	run := func() map[string]string {
		f := newFixture(t, "")
		f.addNode(t, "n1", 4000, 4096)
		f.addNode(t, "n2", 4000, 4096)
		f.addDeployment(t, "web", 4, 1000, 512)
		mustReconcile(t, f.ctl, "web")
		if _, err := f.ctl.StartNodeMaintenance("n1"); err != nil {
			t.Fatal(err)
		}
		return placementSnapshot(t, f)
	}
	a, b := run(), run()
	if len(a) != 4 {
		t.Fatalf("snapshot size = %d", len(a))
	}
	for uid, node := range a {
		if b[uid] != node {
			t.Errorf("pod %s mapped to %s vs %s across identical runs", uid, node, b[uid])
		}
	}
	t.Logf("deterministic post-drain placement: %v", a)
}

// TestMaintenance_InsufficientCapacityWaits: when the remaining healthy nodes
// cannot fit the drain, the overflow waits with a distinguishable reason
// instead of being dropped or force-placed; later reconciliation retries and
// eventually binds when capacity appears.
func TestMaintenance_InsufficientCapacityWaits(t *testing.T) {
	f := newFixture(t, "")
	f.addNode(t, "n1", 8000, 8192) // big node: hosts all 4 initially
	f.addNode(t, "n2", 2000, 8192) // small node: fits at most 2
	f.addDeployment(t, "web", 4, 1000, 512)
	mustReconcile(t, f.ctl, "web")

	res, err := f.ctl.StartNodeMaintenance("n1")
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(res.Migrated) != 2 || len(res.Waiting) != 2 {
		t.Fatalf("drain = %+v, want 2 migrated / 2 waiting", res)
	}
	// The waiting pods are retained with the maintenance-specific reason.
	f.st.View(func(s *cluster.State) {
		for _, p := range s.Pods {
			if p.Phase == cluster.PhaseRescheduling {
				if p.WaitReason != cluster.WaitReasonDraining {
					t.Errorf("waiting pod %s reason=%s, want DrainingForMaintenance", p.UID, p.WaitReason)
				}
				if p.OriginNode != "n1" {
					t.Errorf("waiting pod %s origin=%s, want n1", p.UID, p.OriginNode)
				}
			}
		}
	})
	assertInvariants(t, f, 4)
	counts, total := f.phaseCount(t)
	if counts[cluster.PhaseRunning] != 2 || counts[cluster.PhaseRescheduling] != 2 || total != 4 {
		t.Fatalf("counts=%v total=%d, want 2 running + 2 waiting = 4", counts, total)
	}

	// Repeated drain/reconcile retries but cannot invent capacity: no losses.
	if _, err := f.ctl.StartNodeMaintenance("n1"); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, f.ctl, "web")
	assertInvariants(t, f, 4)

	// Capacity appears (a new healthy node): even WITHOUT an explicit
	// reconcile, a resumed drain pass picks the waiters up itself and binds
	// them — maintenance progress continues, the waiters are not forgotten.
	f.addNode(t, "n3", 8000, 8192)
	res3, err := f.ctl.StartNodeMaintenance("n1")
	if err != nil {
		t.Fatalf("resumed drain after capacity appeared: %v", err)
	}
	if len(res3.Rebound) != 2 || len(res3.Waiting) != 0 {
		t.Fatalf("resumed drain = %+v, want 2 rebound / 0 waiting", res3)
	}
	counts, total = f.phaseCount(t)
	if counts[cluster.PhaseRunning] != 4 || total != 4 {
		t.Fatalf("counts=%v total=%d, want all 4 running once capacity appears", counts, total)
	}
	assertInvariants(t, f, 4)
}

// TestMaintenance_PersistsAcrossRestart: the cordon and an in-flight drain
// survive a process restart; the resumed run continues (never restarts, never
// double-migrates), and the waiters only bind after maintenance ends.
func TestMaintenance_PersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "state.json")

	func() {
		f := newFixture(t, file)
		f.addNode(t, "n1", 8000, 8192)
		f.addNode(t, "n2", 2000, 8192)
		f.addDeployment(t, "web", 4, 1000, 512)
		mustReconcile(t, f.ctl, "web")
		res, err := f.ctl.StartNodeMaintenance("n1")
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Migrated) != 2 || len(res.Waiting) != 2 {
			t.Fatalf("pre-restart drain migrated=%d waiting=%d, want 2/2",
				len(res.Migrated), len(res.Waiting))
		}
	}()

	// "Process restart": fresh store/controller loaded from the JSON file.
	f2 := newFixture(t, file)
	f2.st.View(func(s *cluster.State) {
		if !s.Nodes["n1"].UnderMaintenance {
			t.Fatal("maintenance flag lost across restart")
		}
	})
	// The drain resumes: the flag is already set and no Running pod remains on
	// n1, so nothing is migrated a second time. With no spare capacity the two
	// drain-origin waiters are retried and remain waiting.
	res, err := f2.ctl.StartNodeMaintenance("n1")
	if err != nil {
		t.Fatalf("resumed drain: %v", err)
	}
	if !res.FlagAlreadySet || len(res.Migrated) != 0 || len(res.Rebound) != 0 || len(res.Waiting) != 2 {
		t.Fatalf("resumed drain = %+v, want 0 migrated/rebound and the same 2 waiting", res)
	}
	// Reconcile still cannot put waiters onto the cordoned node.
	mustReconcile(t, f2.ctl, "web")
	f2.st.View(func(s *cluster.State) {
		for _, p := range s.Pods {
			if p.NodeName == "n1" {
				t.Errorf("pod %s bound to still-cordoned n1 after restart", p.UID)
			}
		}
	})
	assertInvariants(t, f2, 4)

	// Maintenance ends after restart: node re-enters the pool and waiters bind.
	changed, err := f2.ctl.EndNodeMaintenance("n1")
	if err != nil || !changed {
		t.Fatalf("end maintenance changed=%v err=%v", changed, err)
	}
	mustReconcile(t, f2.ctl, "web")
	counts, total := f2.phaseCount(t)
	if counts[cluster.PhaseRunning] != 4 || total != 4 {
		t.Fatalf("post-maintenance counts=%v total=%d, want 4 running", counts, total)
	}
	assertInvariants(t, f2, 4)
}

// TestMaintenance_RejoinNoImmediateMoveback: once maintenance ends the node is
// eligible again, but already-migrated pods are NOT pulled back (anti-flap);
// only genuinely-waiting pods may (re)bind, via normal deterministic choice.
func TestMaintenance_RejoinNoImmediateMoveback(t *testing.T) {
	f := newFixture(t, "")
	f.addNode(t, "n1", 8000, 8192)
	f.addNode(t, "n2", 2000, 8192) // only 2 of the 4 pods fit here
	f.addDeployment(t, "web", 4, 1000, 512)
	mustReconcile(t, f.ctl, "web")

	if _, err := f.ctl.StartNodeMaintenance("n1"); err != nil {
		t.Fatal(err)
	}
	// Two moved to n2, two wait (n1 is big, n2 fits only 2).
	drained := placementSnapshot(t, f)
	counts, _ := f.phaseCount(t)
	if counts[cluster.PhaseRunning] != 2 || counts[cluster.PhaseRescheduling] != 2 {
		t.Fatalf("after drain counts=%v, want 2 running / 2 waiting", counts)
	}
	// Record the pods the drain successfully migrated (still bound afterwards).
	var migrated []string
	for uid, node := range drained {
		if node != "" {
			migrated = append(migrated, uid)
		}
	}
	if len(migrated) != 2 {
		t.Fatalf("migrated=%v, want exactly 2", migrated)
	}

	changed, err := f.ctl.EndNodeMaintenance("n1")
	if err != nil || !changed {
		t.Fatalf("end maintenance changed=%v err=%v", changed, err)
	}

	// Ending maintenance itself moves nothing.
	after := placementSnapshot(t, f)
	for _, uid := range migrated {
		if after[uid] != drained[uid] {
			t.Errorf("migrated running pod %s moved during uncordon: %s -> %s", uid, drained[uid], after[uid])
		}
	}

	// Reconcile: waiting pods may now bind onto the recovered n1 (they are not
	// "moved back", they were still unbound); migrated pods stay where they
	// were placed by the drain.
	mustReconcile(t, f.ctl, "web")
	final := placementSnapshot(t, f)
	t.Logf("anti-flap placement drained=%v afterUncordon=%v final=%v", drained, after, final)
	for _, uid := range migrated {
		if final[uid] != drained[uid] {
			t.Errorf("migrated pod %s ended on %s, want it to stay on %s (no immediate move-back)", uid, final[uid], drained[uid])
		}
	}
	f.st.View(func(s *cluster.State) {
		if s.Nodes["n1"].UnderMaintenance {
			t.Error("flag should be cleared after maintenance ends")
		}
		for _, p := range s.Pods {
			if p.Phase != cluster.PhaseRunning {
				t.Errorf("pod %s still %s after rejoin", p.UID, p.Phase)
			}
		}
	})
	assertInvariants(t, f, 4)

	// A further reconcile causes zero churn: placements are now stable.
	stable := placementSnapshot(t, f)
	mustReconcile(t, f.ctl, "web")
	got := placementSnapshot(t, f)
	if len(got) != len(stable) {
		t.Fatal("placement churned on repeated reconcile after rejoin")
	}
	for uid, node := range stable {
		if got[uid] != node {
			t.Errorf("pod %s flapped %s -> %s after convergence", uid, node, got[uid])
		}
	}
}

// TestMaintenance_DetectorHandsOff: while a node is cordoned, stale heartbeats
// must not make the failure detector mark/evict it; after maintenance ends the
// detector owns readiness again. Also covers the NodeLost wait reason on the
// unplanned path so the two wait reasons stay distinguishable.
func TestMaintenance_DetectorHandsOff(t *testing.T) {
	f := newFixture(t, "")
	f.addNode(t, "n1", 4000, 4096)
	f.addNode(t, "n2", 4000, 4096)
	f.addDeployment(t, "web", 4, 1000, 512)
	mustReconcile(t, f.ctl, "web")

	if _, err := f.ctl.StartNodeMaintenance("n1"); err != nil {
		t.Fatal(err)
	}
	// n1 goes fully silent well beyond the eviction grace while cordoned.
	f.clk.advance(100 * time.Second)
	f.st.HeartbeatAt("n2", f.clk.now())
	ev, err := f.ctl.DetectNodeFailures()
	if err != nil || len(ev) != 0 {
		t.Fatalf("detector during maintenance evicted=%v err=%v, want handoff none", ev, err)
	}
	f.st.View(func(s *cluster.State) {
		n := s.Nodes["n1"]
		if !n.Ready || n.Evicted || !n.UnreachableSince.IsZero() {
			t.Fatalf("detector touched cordoned node: ready=%t evicted=%t since=%v", n.Ready, n.Evicted, n.UnreachableSince)
		}
	})

	// End maintenance, node heartbeats again: detector keeps it Ready.
	changed, err := f.ctl.EndNodeMaintenance("n1")
	if err != nil || !changed {
		t.Fatalf("end maintenance changed=%v err=%v", changed, err)
	}
	f.st.HeartbeatAt("n1", f.clk.now())
	if _, err := f.ctl.DetectNodeFailures(); err != nil {
		t.Fatal(err)
	}
	f.st.View(func(s *cluster.State) {
		if !s.Nodes["n1"].Ready {
			t.Fatal("n1 should be Ready after rejoin + fresh heartbeat")
		}
	})

	// Unplanned loss of the other node carries the NodeLost reason, distinct
	// from the planned-drain reason.
	f.clk.advance(100 * time.Second)
	f.st.HeartbeatAt("n1", f.clk.now()) // n1 stays alive; n2 goes silent
	lost, err := f.ctl.DetectNodeFailures()
	if err != nil || len(lost) == 0 {
		t.Fatalf("eviction of silent n2: lost=%v err=%v", lost, err)
	}
	f.st.View(func(s *cluster.State) {
		for _, p := range s.Pods {
			if p.Phase == cluster.PhaseRescheduling && p.WaitReason != cluster.WaitReasonNodeLost {
				t.Errorf("unplanned-loss pod %s reason=%s, want NodeLost", p.UID, p.WaitReason)
			}
		}
	})
}

// TestMaintenance_ConcurrentCordonReconcileUncordon hammers one node with
// interleaved drain / reconcile / uncordon calls. The serialized store must
// keep total/accounting invariants intact (run with -race), and a final drain
// converges with no pod left on the cordoned node.
func TestMaintenance_ConcurrentCordonReconcileUncordon(t *testing.T) {
	f := newFixture(t, "")
	f.addNode(t, "n1", 4000, 4096)
	f.addNode(t, "n2", 4000, 4096)
	f.addDeployment(t, "web", 4, 1000, 512)
	mustReconcile(t, f.ctl, "web")

	var wg sync.WaitGroup
	for g := 0; g < 12; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				switch (g + i) % 3 {
				case 0:
					if _, err := f.ctl.StartNodeMaintenance("n1"); err != nil {
						t.Errorf("drain: %v", err)
						return
					}
				case 1:
					if err := f.st.ScaleDeployment("web", 4); err != nil {
						t.Errorf("scale: %v", err)
						return
					}
					_ = mustReconcile(t, f.ctl, "web")
				case 2:
					if _, err := f.ctl.EndNodeMaintenance("n1"); err != nil {
						t.Errorf("uncordon: %v", err)
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()

	// Converge: cordon again and let all waiters settle; totals must be exact.
	res, err := f.ctl.StartNodeMaintenance("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.StillRunning) != 0 {
		t.Fatalf("converged drain still has %d running pods on n1: %v", len(res.StillRunning), res.StillRunning)
	}
	assertInvariants(t, f, 4)
	if _, err := f.ctl.EndNodeMaintenance("n1"); err != nil {
		t.Fatal(err)
	}
}

// TestMaintenance_UnknownNode surfaces a typed error and changes nothing.
func TestMaintenance_UnknownNode(t *testing.T) {
	f := newFixture(t, "")
	if _, err := f.ctl.StartNodeMaintenance("ghost"); !errors.As(err, new(*ErrNodeNotFound)) {
		t.Fatalf("start on ghost: want ErrNodeNotFound, got %v", err)
	}
	if _, err := f.ctl.EndNodeMaintenance("ghost"); !errors.As(err, new(*ErrNodeNotFound)) {
		t.Fatalf("end on ghost: want ErrNodeNotFound, got %v", err)
	}
}
