package controller

import (
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

func nodeReady(f *fixture, nodeName string) bool {
	var r bool
	f.st.View(func(s *cluster.State) { r = s.Nodes[nodeName].Ready })
	return r
}

// waitReasonOf returns the persisted WaitReason of a pod ("" when bound).
func waitReasonOf(f *fixture, uid string) cluster.WaitReason {
	var w cluster.WaitReason
	f.st.View(func(s *cluster.State) { w = s.Pods[uid].WaitReason })
	return w
}

// assertClusterConsistent checks the global invariants that must hold at ANY
// quiescent point: exactly wantTotal non-terminal pods exist, every Running
// pod sits on a healthy, non-maintenance node, no unbound pod keeps a stale
// NodeName, and every node's Used exactly equals the sum of its pods'
// requests (nothing lost, nothing oversold, nothing double-counted).
func assertClusterConsistent(t *testing.T, f *fixture, wantTotal int) {
	t.Helper()
	f.st.View(func(s *cluster.State) {
		total := 0
		usedByNode := map[string]cluster.Resources{}
		for _, p := range s.Pods {
			if p.Phase == cluster.PhaseFailed {
				continue
			}
			total++
			if p.Phase == cluster.PhaseRunning {
				n := s.Nodes[p.NodeName]
				if n == nil {
					t.Errorf("pod %s bound to unknown node %q", p.UID, p.NodeName)
					continue
				}
				if n.Maintenance {
					t.Errorf("pod %s running on maintenance node %s", p.UID, p.NodeName)
				}
				if !n.Ready {
					t.Errorf("pod %s running on not-ready node %s", p.UID, p.NodeName)
				}
				usedByNode[p.NodeName] = usedByNode[p.NodeName].Add(p.Requests)
			} else if p.NodeName != "" {
				t.Errorf("unbound pod %s (%s) still has nodeName=%q", p.UID, p.Phase, p.NodeName)
			}
		}
		if total != wantTotal {
			t.Errorf("non-terminal pod total = %d, want %d", total, wantTotal)
		}
		for name, n := range s.Nodes {
			if n.Used != usedByNode[name] {
				t.Errorf("node %s Used=%v but bound pods sum to %v", name, n.Used, usedByNode[name])
			}
		}
	})
}

// 计划内维护迁移：进入维护的同一个调用内，实例就被真正迁到健康节点，
// 不需要再额外触发一次调谐；实例总数与逐节点账目始终对得上。
func TestMaintenance_DrainMigratesToHealthyNodes(t *testing.T) {
	t.Log("scenario: 计划内维护 -> 同一次调用内排干并迁移到健康节点，无需额外触发")
	f := newFixture(t, "")
	f.addNode(t, "n1", 4000, 4096)
	f.addNode(t, "n2", 4000, 4096)
	f.addDeployment(t, "web", 4, 1000, 512)
	mustReconcile(t, f.ctl, "web")

	// Deterministic pre-drain placement: web-0/web-2 on n1, web-1/web-3 on n2.
	if got := podsOnNode(f, "n1"); len(got) != 2 {
		t.Fatalf("pre-drain n1 pods = %v, want 2", got)
	}

	res, err := f.ctl.EnterMaintenance("n1")
	if err != nil {
		t.Fatalf("enter maintenance: %v", err)
	}
	t.Logf("case 一次性迁移: drained=%v migrated=%v waiting=%v", res.Drained, res.Migrated, res.Waiting)
	if len(res.Drained) != 2 || len(res.Migrated) != 2 || len(res.Waiting) != 0 {
		t.Fatalf("result = %+v, want 2 drained AND 2 migrated in the SAME call", res)
	}

	// Immediately after EnterMaintenance (no extra reconcile!): all 4 running,
	// n1 empty, everything already on the healthy node.
	counts, total := f.phaseCount(t)
	if total != 4 || counts[cluster.PhaseRunning] != 4 {
		t.Fatalf("post-maintenance counts=%v total=%d, want 4 running WITHOUT any extra reconcile", counts, total)
	}
	if got := podsOnNode(f, "n1"); len(got) != 0 {
		t.Fatalf("maintenance node n1 still runs %v, want none", got)
	}
	if got := podsOnNode(f, "n2"); len(got) != 4 {
		t.Fatalf("n2 pods = %v, want all 4 migrated there", got)
	}
	assertClusterConsistent(t, f, 4)

	// A follow-up reconcile is a pure no-op: migration already happened.
	r := mustReconcile(t, f.ctl, "web")
	if len(r.Bound) != 0 || len(r.Created) != 0 {
		t.Fatalf("reconcile after maintenance churned: bound=%v created=%v, want zero", r.Bound, r.Created)
	}

	// New replicas must not land on the maintenance node either (n2 is full,
	// so the 5th replica waits as Pending — never forced onto n1).
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

	// Re-entering maintenance is idempotent: nothing left to drain or migrate.
	again, err := f.ctl.EnterMaintenance("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Drained) != 0 || len(again.Migrated) != 0 {
		t.Fatalf("re-enter = %+v, want idempotent empty", again)
	}
	t.Log("result: PASS — 同调用内完成迁移，后续调谐零抖动，重复进入幂等")
}

// 目标容量放不下：放不下的实例以可区分的原因等待（维护节点被拒 vs 健康
// 节点满载），不丢实例，也不硬塞回维护节点；扩容后等待实例被放置。
func TestMaintenance_InsufficientCapacityWaitsWithReason(t *testing.T) {
	t.Log("scenario: 维护排干但健康节点容量不足 -> 可区分原因等待，不丢不硬塞")
	f := newFixture(t, "")
	f.addNode(t, "n1", 4000, 4096)
	f.addNode(t, "n2", 2000, 4096) // fits only 2 pods of 1000m
	f.addDeployment(t, "web", 3, 1000, 512)
	mustReconcile(t, f.ctl, "web")
	// Placement: web-0->n1, web-1->n2, web-2->n1 (n1 has more remaining CPU).

	res, err := f.ctl.EnterMaintenance("n1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("case 容量不足等待: drained=%v migrated=%v waiting=%v", res.Drained, res.Migrated, res.Waiting)
	// n2 has 1000m free: exactly one drained pod migrates, the other waits.
	if len(res.Drained) != 2 || len(res.Migrated) != 1 || len(res.Waiting) != 1 {
		t.Fatalf("result = %+v, want 2 drained / 1 migrated / 1 waiting", res)
	}
	if len(res.WaitErrors) != 1 {
		t.Fatalf("wait errors = %d, want 1", len(res.WaitErrors))
	}
	se := res.WaitErrors[0]
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
	// The waiting pod carries the persisted maintenance-drain wait cause.
	if w := waitReasonOf(f, res.Waiting[0]); w != cluster.WaitMaintenanceDrain {
		t.Fatalf("waiting pod waitReason = %q, want %q", w, cluster.WaitMaintenanceDrain)
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
	assertClusterConsistent(t, f, 3)

	// Once capacity appears, a reconcile places the waiting pod and consumes
	// its wait reason.
	f.addNode(t, "n3", 4000, 4096)
	mustReconcile(t, f.ctl, "web")
	counts, total = f.phaseCount(t)
	if total != 3 || counts[cluster.PhaseRunning] != 3 {
		t.Fatalf("after adding capacity counts=%v total=%d, want 3 running", counts, total)
	}
	if w := waitReasonOf(f, res.Waiting[0]); w != "" {
		t.Fatalf("bound pod still carries waitReason %q, want cleared", w)
	}
	assertClusterConsistent(t, f, 3)
	t.Log("result: PASS — 等待原因可区分（NodeMaintenance vs InsufficientCPU），扩容后放置")
}

// 维护标记、排干/等待进度跨进程重启保持：重启后维护节点仍被摘除、等待
// 实例的维护原因仍可辨认，进度接着往下走而不是重来。
func TestMaintenance_FlagPersistedAcrossRestart(t *testing.T) {
	t.Log("scenario: 维护进行中进程重启 -> 标记/等待原因/进度全部从磁盘恢复")
	dir := t.TempDir()
	file := filepath.Join(dir, "state.json")

	func() {
		f := newFixture(t, file)
		f.addNode(t, "n1", 1000, 4096)
		f.addNode(t, "n2", 1000, 4096) // fits exactly 1 pod
		f.addDeployment(t, "web", 2, 1000, 512)
		mustReconcile(t, f.ctl, "web") // web-0->n1, web-1->n2 (tie broken by name)
		res, err := f.ctl.EnterMaintenance("n1")
		if err != nil {
			t.Fatal(err)
		}
		// n2 is full, so the drained pod cannot migrate yet: it waits.
		if len(res.Drained) != 1 || len(res.Waiting) != 1 {
			t.Fatalf("pre-restart result = %+v, want 1 drained + 1 waiting", res)
		}
		// Crash/restart while the migration is still pending.
	}()

	// Fresh process: flag, wait reason and progress must all survive.
	f2 := newFixture(t, file)
	if !nodeMaintenance(f2, "n1") {
		t.Fatal("maintenance flag lost across restart")
	}
	counts, total := f2.phaseCount(t)
	if total != 2 || counts[cluster.PhaseRescheduling] != 1 {
		t.Fatalf("after restart counts=%v total=%d, want 1 rescheduling of 2 (progress kept)", counts, total)
	}
	if w := waitReasonOf(f2, "default-web-0"); w != cluster.WaitMaintenanceDrain {
		t.Fatalf("after restart waitReason = %q, want persisted %q", w, cluster.WaitMaintenanceDrain)
	}

	// Progress continues, never restarts: adding capacity lets the SAME
	// waiting pod bind (no re-drain, no duplicate).
	f2.addNode(t, "n3", 4000, 4096)
	r := mustReconcile(t, f2.ctl, "web")
	if len(r.Created) != 0 || len(r.Bound) != 1 {
		t.Fatalf("post-restart reconcile created=%v bound=%v, want 0 created + 1 bound", r.Created, r.Bound)
	}
	if got := podsOnNode(f2, "n1"); len(got) != 0 {
		t.Fatalf("after restart n1 pods = %v, want 0", got)
	}
	counts, _ = f2.phaseCount(t)
	if counts[cluster.PhaseRunning] != 2 {
		t.Fatalf("after restart+reconcile running = %d, want 2", counts[cluster.PhaseRunning])
	}

	// Re-entering maintenance after restart is idempotent (nothing to drain).
	again, err := f2.ctl.EnterMaintenance("n1")
	if err != nil || len(again.Drained) != 0 {
		t.Fatalf("re-enter after restart = %+v err=%v, want empty", again, err)
	}
	assertClusterConsistent(t, f2, 2)
	t.Log("result: PASS — 重启后标记/原因/进度完整恢复，迁移不重来不重复")
}

// 恢复再入与防抖动：心跳新鲜的节点退出维护后立即重新参与调度，但已迁走
// 的实例绝不搬回；故障检测器也不会因陈旧历史误判。
func TestMaintenance_ExitRejoinsWithoutFlap(t *testing.T) {
	t.Log("scenario: 节点真实恢复后退出维护 -> 立即再入调度，运行实例不搬回（防抖动）")
	f := newFixture(t, "")
	f.addNode(t, "n1", 4000, 4096)
	f.addNode(t, "n2", 4000, 4096)
	f.addDeployment(t, "web", 4, 1000, 512)
	mustReconcile(t, f.ctl, "web")

	if _, err := f.ctl.EnterMaintenance("n1"); err != nil {
		t.Fatal(err)
	}
	if got := podsOnNode(f, "n2"); len(got) != 4 {
		t.Fatalf("n2 pods = %v, want all 4 after drain+migrate", got)
	}

	// Node comes back while its heartbeat is still fresh.
	rejoined, err := f.ctl.ExitMaintenance("n1")
	if err != nil {
		t.Fatal(err)
	}
	if !rejoined {
		t.Fatal("fresh-heartbeat node must rejoin scheduling")
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

	// The failure detector must not evict the just-recovered node.
	f.clk.advance(5 * time.Second) // within heartbeat timeout of the fresh LastSeen
	f.st.HeartbeatAt("n2", f.clk.now())
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
	assertClusterConsistent(t, f, 6)

	// Exiting again is a no-op; repeated reconciles stay stable (no flap).
	if _, err := f.ctl.ExitMaintenance("n1"); err != nil {
		t.Fatal(err)
	}
	res = mustReconcile(t, f.ctl, "web")
	if len(res.Bound) != 0 || len(res.Created) != 0 {
		t.Fatalf("post-exit reconcile churn: bound=%v created=%v, want none", res.Bound, res.Created)
	}
	t.Log("result: PASS — 恢复再入零抖动，新实例按确定性规则落到恢复节点")
}

// 未真正恢复的节点退出维护时绝不能被当成健康：标记清除但保持不可调度，
// 由故障检测器按既有失联路径接管；只有真实心跳到达后才重新参与调度。
// 整个过程中不存在“先放上去又被驱逐”的来回搬。
func TestMaintenance_ExitWhileSilentDoesNotRejoin(t *testing.T) {
	t.Log("scenario: 节点维护期间失联 -> 退出维护不算恢复，真实心跳后才再入")
	f := newFixture(t, "")
	f.addNode(t, "n1", 4000, 4096)
	f.addNode(t, "n2", 4000, 4096)
	f.addDeployment(t, "web", 4, 1000, 512)
	mustReconcile(t, f.ctl, "web")

	if _, err := f.ctl.EnterMaintenance("n1"); err != nil {
		t.Fatal(err)
	}
	if got := podsOnNode(f, "n2"); len(got) != 4 {
		t.Fatalf("n2 pods = %v, want all 4 migrated", got)
	}

	// n1 stays silent past the heartbeat timeout while in maintenance.
	f.clk.advance(15 * time.Second)
	f.st.HeartbeatAt("n2", f.clk.now())

	rejoined, err := f.ctl.ExitMaintenance("n1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("case 静默退出: rejoined=%t (want false)", rejoined)
	if rejoined {
		t.Fatal("a node that never heartbeated again must NOT be treated as recovered")
	}
	if nodeMaintenance(f, "n1") {
		t.Fatal("maintenance flag must be cleared even when the node is not healthy")
	}
	if nodeReady(f, "n1") {
		t.Fatal("silently-returned node must stay NotReady")
	}

	// New replicas must NOT land on the not-really-recovered node: n2 is
	// full, n1 is NotReady -> the 5th replica waits as Pending. No
	// "placed then evicted" ping-pong can occur.
	if err := f.st.ScaleDeployment("web", 5); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, f.ctl, "web")
	if got := podsOnNode(f, "n1"); len(got) != 0 {
		t.Fatalf("pod placed on silently-returned node: %v — this is the ping-pong we must prevent", got)
	}
	counts, total := f.phaseCount(t)
	if total != 5 || counts[cluster.PhasePending] != 1 {
		t.Fatalf("counts=%v total=%d, want 4 running + 1 pending = 5", counts, total)
	}

	// The failure detector owns the node now: within grace it only waits.
	ev, err := f.ctl.DetectNodeFailures()
	if err != nil || len(ev) != 0 {
		t.Fatalf("detection within grace evicted=%v err=%v, want none", ev, err)
	}
	// Past grace the node is declared lost (nothing to evict — idempotent),
	// and STILL no pod has ever been placed on it.
	f.clk.advance(25 * time.Second)
	f.st.HeartbeatAt("n2", f.clk.now())
	if _, err := f.ctl.DetectNodeFailures(); err != nil {
		t.Fatal(err)
	}
	if got := podsOnNode(f, "n1"); len(got) != 0 {
		t.Fatalf("n1 pods = %v, want 0 throughout the silent window", got)
	}

	// The node ACTUALLY recovers: a real heartbeat arrives and the detector
	// restores it to Ready — only now may it rejoin scheduling.
	f.st.HeartbeatAt("n1", f.clk.now())
	if _, err := f.ctl.DetectNodeFailures(); err != nil {
		t.Fatal(err)
	}
	if !nodeReady(f, "n1") {
		t.Fatal("node with a fresh heartbeat must be restored to Ready by the detector")
	}
	res := mustReconcile(t, f.ctl, "web")
	if len(res.Bound) != 1 {
		t.Fatalf("after real recovery bound=%v, want the 1 waiting pod", res.Bound)
	}
	if got := podsOnNode(f, "n1"); len(got) != 1 {
		t.Fatalf("n1 pods = %v, want the waiting pod placed only AFTER real recovery", got)
	}
	assertClusterConsistent(t, f, 5)
	t.Log("result: PASS — 静默节点退出维护不被误判健康，真实恢复后才再入，无来回搬")
}

// 维护等待与失联等待的原因可区分，且跨进程重启仍可辨认；绑定后原因被消费。
func TestMaintenance_WaitReasonDistinctAndPersisted(t *testing.T) {
	t.Log("scenario: 维护排干等待 vs 失联驱逐等待 -> 原因可区分且重启后仍可辨认")
	dir := t.TempDir()
	file := filepath.Join(dir, "state.json")

	func() {
		f := newFixture(t, file)
		f.addNode(t, "n1", 1000, 4096)
		f.addNode(t, "n2", 1000, 4096)
		f.addDeployment(t, "web", 1, 1000, 512)  // lands on n1
		f.addDeployment(t, "batch", 1, 1000, 512) // lands on n2
		mustReconcile(t, f.ctl, "web")
		mustReconcile(t, f.ctl, "batch")

		// Maintenance drain: web's pod waits (n2 is full) as MaintenanceDrain.
		res, err := f.ctl.EnterMaintenance("n1")
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Waiting) != 1 {
			t.Fatalf("maintenance result = %+v, want 1 waiting", res)
		}
		if w := waitReasonOf(f, "default-web-0"); w != cluster.WaitMaintenanceDrain {
			t.Fatalf("web waitReason = %q, want %q", w, cluster.WaitMaintenanceDrain)
		}

		// Failure eviction: n2 goes silent past grace -> batch's pod waits as
		// NodeFailure. The two waits must be distinguishable.
		f.clk.advance(45 * time.Second)
		ev, err := f.ctl.DetectNodeFailures()
		if err != nil || len(ev) != 1 {
			t.Fatalf("eviction = %v err=%v, want 1 evicted", ev, err)
		}
		if w := waitReasonOf(f, "default-batch-0"); w != cluster.WaitNodeFailure {
			t.Fatalf("batch waitReason = %q, want %q", w, cluster.WaitNodeFailure)
		}
		if w := waitReasonOf(f, "default-web-0"); w != cluster.WaitMaintenanceDrain {
			t.Fatalf("web waitReason = %q, must stay %q", w, cluster.WaitMaintenanceDrain)
		}
		t.Log("case 原因区分: web=MaintenanceDrain batch=NodeFailure")
		// Crash/restart while both waits are pending.
	}()

	// After a process restart the two wait causes are still distinguishable.
	f2 := newFixture(t, file)
	if w := waitReasonOf(f2, "default-web-0"); w != cluster.WaitMaintenanceDrain {
		t.Fatalf("after restart web waitReason = %q, want %q", w, cluster.WaitMaintenanceDrain)
	}
	if w := waitReasonOf(f2, "default-batch-0"); w != cluster.WaitNodeFailure {
		t.Fatalf("after restart batch waitReason = %q, want %q", w, cluster.WaitNodeFailure)
	}
	counts, total := f2.phaseCount(t)
	if total != 2 || counts[cluster.PhaseRescheduling] != 2 {
		t.Fatalf("after restart counts=%v total=%d, want 2 rescheduling of 2", counts, total)
	}

	// New capacity lets both bind; binding consumes the wait reason.
	f2.addNode(t, "n3", 4000, 4096)
	mustReconcile(t, f2.ctl, "web")
	mustReconcile(t, f2.ctl, "batch")
	for _, uid := range []string{"default-web-0", "default-batch-0"} {
		if w := waitReasonOf(f2, uid); w != "" {
			t.Fatalf("bound pod %s still carries waitReason %q", uid, w)
		}
	}
	assertClusterConsistent(t, f2, 2)
	t.Log("result: PASS — 两类等待原因重启后仍可区分，绑定后原因清除")
}

// 维护中的节点被故障检测器整体跳过：不标记不可达、不驱逐，避免双重处理。
func TestMaintenance_SkippedByFailureDetector(t *testing.T) {
	t.Log("scenario: 维护节点静默超宽限 -> 故障检测器跳过，不双重处理")
	f := newFixture(t, "")
	f.addNode(t, "n1", 4000, 4096)
	f.addNode(t, "n2", 4000, 4096)
	f.addDeployment(t, "web", 2, 1000, 512)
	mustReconcile(t, f.ctl, "web")

	if _, err := f.ctl.EnterMaintenance("n1"); err != nil {
		t.Fatal(err)
	}

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
	t.Log("result: PASS — 维护节点不被故障检测双重处理")
}

// 并发进入/退出维护与调谐：Store 串行化一切，实例总数与逐节点账目在任何
// 静止点都对得上；风暴过后能收敛到确定的终态。
func TestMaintenance_ConcurrentEnterExitConsistent(t *testing.T) {
	t.Log("scenario: 并发维护/恢复/调谐风暴 -> 任何时刻账目一致，终态确定")
	f := newFixture(t, "")
	f.addNode(t, "n1", 8000, 8192)
	f.addNode(t, "n2", 8000, 8192)
	f.addDeployment(t, "web", 8, 500, 256)
	mustReconcile(t, f.ctl, "web")

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(3)
		go func() { defer wg.Done(); _, _ = f.ctl.EnterMaintenance("n1") }()
		go func() { defer wg.Done(); _, _ = f.ctl.ExitMaintenance("n1") }()
		go func() { defer wg.Done(); _ = mustReconcile(t, f.ctl, "web") }()
	}
	wg.Wait()

	// Invariant at any quiescent point: exactly the desired number of
	// non-terminal pods exists, and node accounting adds up.
	assertClusterConsistent(t, f, 8)

	// Drive to a deterministic end state: maintenance on -> fully drained and
	// migrated in one call.
	res, err := f.ctl.EnterMaintenance("n1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("case 风暴后收敛: drained=%d migrated=%d waiting=%v", len(res.Drained), len(res.Migrated), res.Waiting)
	if got := podsOnNode(f, "n1"); len(got) != 0 {
		t.Fatalf("final drain: n1 pods = %v, want 0", got)
	}
	counts, total := f.phaseCount(t)
	if total != 8 || counts[cluster.PhaseRunning] != 8 {
		t.Fatalf("final counts=%v total=%d, want 8 running", counts, total)
	}

	// And back out: recovery with zero churn.
	if _, err := f.ctl.ExitMaintenance("n1"); err != nil {
		t.Fatal(err)
	}
	r := mustReconcile(t, f.ctl, "web")
	if len(r.Bound) != 0 || len(r.Created) != 0 {
		t.Fatalf("post-storm exit churn: bound=%v created=%v, want none", r.Bound, r.Created)
	}
	assertClusterConsistent(t, f, 8)
	t.Logf("result: PASS — 并发风暴后 total=%d running=%d 账目一致", total, counts[cluster.PhaseRunning])
}

// 并发维护/恢复的中途进程重启：第一段风暴后重启，第二段风暴接着持久化
// 状态继续——进度不重来、不重复迁移，最终仍能收敛到确定终态。
func TestMaintenance_ConcurrentWithMidwayRestart(t *testing.T) {
	t.Log("scenario: 并发维护/恢复风暴 + 中途重启 -> 状态接续一致，不重来不重复")
	dir := t.TempDir()
	file := filepath.Join(dir, "state.json")

	storm := func(f *fixture) {
		var wg sync.WaitGroup
		for i := 0; i < 6; i++ {
			wg.Add(3)
			go func() { defer wg.Done(); _, _ = f.ctl.EnterMaintenance("n1") }()
			go func() { defer wg.Done(); _, _ = f.ctl.ExitMaintenance("n1") }()
			go func() { defer wg.Done(); _ = mustReconcile(t, f.ctl, "web") }()
		}
		wg.Wait()
	}

	// Phase 1: run a concurrent storm, then "crash".
	func() {
		f := newFixture(t, file)
		f.addNode(t, "n1", 8000, 8192)
		f.addNode(t, "n2", 8000, 8192)
		f.addDeployment(t, "web", 8, 500, 256)
		mustReconcile(t, f.ctl, "web")
		storm(f)
		assertClusterConsistent(t, f, 8)
	}()

	// Midway restart: a fresh process loads the persisted state and the storm
	// continues — maintenance progress must continue, never restart.
	f2 := newFixture(t, file)
	assertClusterConsistent(t, f2, 8)
	storm(f2)
	assertClusterConsistent(t, f2, 8)

	// Converge deterministically. First make sure n1 actually carries pods:
	// exit maintenance (heartbeat still fresh), scale up so new replicas land
	// on the empty n1, THEN drain it — the migrated set must be exactly what
	// was on n1 (no loss, no duplicates).
	if _, err := f2.ctl.ExitMaintenance("n1"); err != nil {
		t.Fatal(err)
	}
	if err := f2.st.ScaleDeployment("web", 12); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, f2.ctl, "web")
	assertClusterConsistent(t, f2, 12)
	before := len(podsOnNode(f2, "n1"))
	if before == 0 {
		t.Fatal("pre-drain: n1 should carry the new replicas, got 0")
	}
	res, err := f2.ctl.EnterMaintenance("n1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("case 重启后收敛: onN1Before=%d drained=%d migrated=%d", before, len(res.Drained), len(res.Migrated))
	if len(res.Drained) != before || len(res.Migrated) != before {
		t.Fatalf("drained=%d migrated=%d, want exactly the %d pods that were on n1 (no loss, no duplicates)",
			len(res.Drained), len(res.Migrated), before)
	}
	if got := podsOnNode(f2, "n1"); len(got) != 0 {
		t.Fatalf("n1 pods = %v, want 0 after final drain", got)
	}
	counts, total := f2.phaseCount(t)
	if total != 12 || counts[cluster.PhaseRunning] != 12 {
		t.Fatalf("final counts=%v total=%d, want 12 running", counts, total)
	}

	// Back out (heartbeat still fresh) and confirm stability.
	rejoined, err := f2.ctl.ExitMaintenance("n1")
	if err != nil || !rejoined {
		t.Fatalf("exit rejoined=%t err=%v, want clean rejoin", rejoined, err)
	}
	r := mustReconcile(t, f2.ctl, "web")
	if len(r.Bound) != 0 || len(r.Created) != 0 {
		t.Fatalf("post-restart exit churn: bound=%v created=%v, want none", r.Bound, r.Created)
	}
	assertClusterConsistent(t, f2, 12)

	// One more restart to prove the converged state itself is durable.
	f3 := newFixture(t, file)
	assertClusterConsistent(t, f3, 12)
	if nodeMaintenance(f3, "n1") {
		t.Fatal("maintenance flag must stay cleared after final restart")
	}
	t.Log("result: PASS — 中途重启后并发进度接续，迁移不重复，终态确定且持久")
}

// 维护中的节点上等待实例的账目：维护排干与失联驱逐同时发生时，实例总数
// 恒等式 Running+Pending+Rescheduling == 期望数 依然成立。
func TestMaintenance_TotalInvariantDuringMixedWaits(t *testing.T) {
	t.Log("scenario: 维护等待与失联等待并存 -> 实例总数恒等式始终成立")
	f := newFixture(t, "")
	f.addNode(t, "n1", 2000, 4096)
	f.addNode(t, "n2", 4000, 4096)
	f.addDeployment(t, "web", 4, 1000, 512)
	mustReconcile(t, f.ctl, "web") // 1 on n1, 3 on n2 (deterministic)

	// Maintain n1: its pod migrates onto n2 (n2 has exactly 1000m free).
	res, err := f.ctl.EnterMaintenance("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Drained) != 1 || len(res.Migrated) != 1 {
		t.Fatalf("drained=%v migrated=%v, want 1/1", res.Drained, res.Migrated)
	}
	assertClusterConsistent(t, f, 4)

	// Now n2 fails silently past grace: all 4 pods evicted, n1 in maintenance
	// and n2 lost -> every pod waits, each with its own distinguishable cause.
	f.clk.advance(45 * time.Second)
	ev, err := f.ctl.DetectNodeFailures()
	if err != nil || len(ev) != 4 {
		t.Fatalf("evicted=%v err=%v, want 4", ev, err)
	}
	mustReconcile(t, f.ctl, "web") // nothing can be placed; all stay waiting
	counts, total := f.phaseCount(t)
	if total != 4 || counts[cluster.PhaseRescheduling] != 4 {
		t.Fatalf("counts=%v total=%d, want 4 rescheduling of 4 (none lost)", counts, total)
	}
	for uid, want := range map[string]cluster.WaitReason{
		// n2's failure eviction must stamp NodeFailure on all four pods
		// (they were migrated to n2 by the maintenance drain earlier).
		"default-web-0": cluster.WaitNodeFailure,
		"default-web-1": cluster.WaitNodeFailure,
		"default-web-2": cluster.WaitNodeFailure,
		"default-web-3": cluster.WaitNodeFailure,
	} {
		if got := waitReasonOf(f, uid); got != want {
			t.Fatalf("pod %s waitReason = %q, want %q", uid, got, want)
		}
	}
	t.Logf("case 混合等待: total=%d rescheduling=%d (Running+Pending+Rescheduling == 期望数 成立)", total, counts[cluster.PhaseRescheduling])

	// n1 exits maintenance while STILL silent: it must not absorb the waiting
	// pods (it never heartbeated). n2 heartbeats back and recovers instead.
	rejoined, err := f.ctl.ExitMaintenance("n1")
	if err != nil {
		t.Fatal(err)
	}
	if rejoined {
		t.Fatal("n1 never heartbeated -> must not rejoin")
	}
	mustReconcile(t, f.ctl, "web")
	if got := podsOnNode(f, "n1"); len(got) != 0 {
		t.Fatalf("waiting pods landed on silently-returned n1: %v", got)
	}
	f.st.HeartbeatAt("n2", f.clk.now())
	if _, err := f.ctl.DetectNodeFailures(); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, f.ctl, "web")
	counts, total = f.phaseCount(t)
	if total != 4 || counts[cluster.PhaseRunning] != 4 {
		t.Fatalf("after n2 recovery counts=%v total=%d, want 4 running", counts, total)
	}
	assertClusterConsistent(t, f, 4)
	t.Log("result: PASS — 混合等待下总数恒等，静默退出维护不吸收等待实例")
}
