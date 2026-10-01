package controller

import (
	"bytes"
	"context"
	"log"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/feng111627/go-cluster-scheduler-go/api"
	"github.com/feng111627/go-cluster-scheduler-go/scheduler"
	"github.com/feng111627/go-cluster-scheduler-go/store"
)

type harness struct {
	t      *testing.T
	clk    *fakeT
	st     *store.Store
	mgr    *Manager
	rec    *Reconciler
	ctx    context.Context
	logbuf *bytes.Buffer
	path   string
}

type fakeT struct{ t time.Time }

func (f *fakeT) now() time.Time          { return f.t }
func (f *fakeT) advance(d time.Duration) { f.t = f.t.Add(d) }

func testCfg() Config {
	return Config{HeartbeatTimeout: 30 * time.Second, NodeGracePeriod: 30 * time.Second, RequeueInterval: time.Hour}
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	clk := &fakeT{t: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := store.New(context.Background(), path)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	var buf bytes.Buffer
	lg := log.New(&buf, "[test] ", log.Lmicroseconds)
	sch := scheduler.New(lg)
	return &harness{
		t: t, clk: clk, st: st, path: path,
		mgr:    NewManager(st, clk.now),
		rec:    New(st, sch, testCfg(), lg, clk.now),
		ctx:    context.Background(),
		logbuf: &buf,
	}
}

// restartHarness closes h and reopens the same state file with fresh
// manager/reconciler instances, simulating a process restart.
func restartHarness(t *testing.T, h *harness) *harness {
	t.Helper()
	if err := h.st.Close(); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	lg := log.New(&buf, "[test-restart] ", log.Lmicroseconds)
	st, err := store.New(context.Background(), h.path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	sch := scheduler.New(lg)
	return &harness{
		t: h.t, clk: h.clk, st: st, path: h.path,
		mgr:    NewManager(st, h.clk.now),
		rec:    New(st, sch, testCfg(), lg, h.clk.now),
		ctx:    context.Background(),
		logbuf: &buf,
	}
}

func (h *harness) addNode(name string, cpu, mem int64, taints ...api.Taint) {
	h.t.Helper()
	if err := h.mgr.AddNode(h.ctx, api.Node{
		Name: name, Capacity: api.Resources{CPU: cpu, Memory: mem}, Taints: taints,
	}); err != nil {
		h.t.Fatalf("add node %s: %v", name, err)
	}
}

func (h *harness) applyRS(rs api.ReplicaSet) {
	h.t.Helper()
	if err := h.mgr.ApplyReplicaSet(h.ctx, rs); err != nil {
		h.t.Fatalf("apply rs: %v", err)
	}
}

func (h *harness) reconcile(label string) *Report {
	h.t.Helper()
	rep, err := h.rec.ReconcileOnce(h.ctx, "")
	if err != nil {
		h.t.Fatalf("[%s] reconcile: %v", label, err)
	}
	h.t.Logf("=== %s ===\n%s", label, h.logbuf.String())
	h.logbuf.Reset()
	return rep
}

func webRS(replicas int, req api.Resources, tols ...api.Toleration) api.ReplicaSet {
	return api.ReplicaSet{Name: "web", Namespace: "default", Replicas: replicas,
		Template: api.PodSpec{NamePrefix: "web", Request: req, Tolerations: tols}}
}

func (h *harness) activePods(rsName string) []api.Pod {
	var out []api.Pod
	for _, p := range h.mgr.ListPods() {
		if p.OwnerReplica == rsName && !p.Phase.IsTerminal() {
			out = append(out, p)
		}
	}
	return out
}

func (h *harness) runningOn(node string) int {
	n := 0
	for _, p := range h.mgr.ListPods() {
		if p.NodeName == node && p.Phase == api.PodRunning {
			n++
		}
	}
	return n
}

func findEvent(rep *Report, kind string) *Event {
	for i := range rep.Events {
		if rep.Events[i].Kind == kind {
			return &rep.Events[i]
		}
	}
	return nil
}

func TestReconcile_ScheduleAndIdempotence(t *testing.T) {
	h := newHarness(t)
	h.addNode("n1", 100, 100)
	h.addNode("n2", 100, 100)
	h.applyRS(webRS(2, api.Resources{CPU: 10, Memory: 10}))

	rep := h.reconcile("initial")
	if rep.Counts.Running != 2 {
		t.Fatalf("running = %d, want 2", rep.Counts.Running)
	}
	if rep.ActivePerRS["web"] != 2 {
		t.Fatalf("active web = %d, want 2", rep.ActivePerRS["web"])
	}

	// Second pass: no create/evict/schedule events, same pod UIDs.
	before := h.activePods("web")
	rep2 := h.reconcile("repeated")
	if len(rep2.Events) != 0 {
		t.Fatalf("repeated reconcile produced events: %+v", rep2.Events)
	}
	after := h.activePods("web")
	if len(before) != len(after) {
		t.Fatalf("active count changed: %d -> %d", len(before), len(after))
	}
	for i := range before {
		if before[i].UID != after[i].UID {
			t.Fatalf("pod identity changed: %s -> %s", before[i].UID, after[i].UID)
		}
	}
}

func TestReconcile_ScaleInRemovesHighestSlots(t *testing.T) {
	h := newHarness(t)
	h.addNode("n1", 1000, 1000)
	h.applyRS(webRS(4, api.Resources{CPU: 1, Memory: 1}))
	h.reconcile("scale to 4")

	h.applyRS(webRS(2, api.Resources{CPU: 1, Memory: 1}))
	rep := h.reconcile("scale to 2")
	// Exactly two scale-down events, slots 3 then 2 (highest first).
	var slots []int
	for _, e := range rep.Events {
		if e.Kind == EvScaleDown {
			slots = append(slots, e.Slot)
		}
	}
	if len(slots) != 2 || slots[0] != 3 || slots[1] != 2 {
		t.Fatalf("scale-down slots = %v, want [3 2]", slots)
	}
	if rep.ActivePerRS["web"] != 2 {
		t.Fatalf("active = %d, want 2", rep.ActivePerRS["web"])
	}
	h.reconcile("after scale-in idempotent")
}

func TestReconcile_UnschedulableStaysPendingNoOverCreation(t *testing.T) {
	h := newHarness(t)
	h.addNode("n1", 10, 10)
	h.applyRS(webRS(2, api.Resources{CPU: 8, Memory: 8})) // only 1 fits

	rep := h.reconcile("first pass")
	if rep.Counts.Running != 1 || rep.Counts.Pending != 1 {
		t.Fatalf("want 1 running + 1 pending, got %+v", rep.Counts)
	}
	if findEvent(rep, EvUnschedulable) == nil {
		t.Fatal("expected Unschedulable event")
	}
	// Repeated passes must not mint extra pods for the blocked slot.
	rep2 := h.reconcile("retry still blocked")
	creates := 0
	for _, e := range rep2.Events {
		if e.Kind == EvCreate {
			creates++
		}
	}
	if creates != 0 {
		t.Fatalf("retry created %d extra pods", creates)
	}
	if rep2.ActivePerRS["web"] != 2 { // 1 running + 1 pending
		t.Fatalf("active = %d, want 2", rep2.ActivePerRS["web"])
	}
}

func TestReconcile_PendingGetsScheduledWhenCapacityFrees(t *testing.T) {
	h := newHarness(t)
	h.addNode("n1", 10, 10)
	h.addNode("n2", 10, 10)
	h.applyRS(webRS(3, api.Resources{CPU: 8, Memory: 8})) // n1 and n2 hold one each, 1 pending
	rep := h.reconcile("two placed one pending")
	if rep.Counts.Pending != 1 {
		t.Fatalf("want 1 pending, got %d", rep.Counts.Pending)
	}
	h.addNode("n3", 10, 10)
	rep2 := h.reconcile("new node appears")
	if rep2.Counts.Pending != 0 || rep2.Counts.Running != 3 {
		t.Fatalf("want 3 running, got %+v", rep2.Counts)
	}
}

func TestReconciler_FailoverAfterGrace(t *testing.T) {
	h := newHarness(t)
	h.addNode("n1", 100, 100)
	h.addNode("n2", 100, 100)
	h.applyRS(webRS(2, api.Resources{CPU: 10, Memory: 10}))
	h.reconcile("initial")

	// Locate the pod on n2 and remember it.
	var victimUID string
	for _, p := range h.activePods("web") {
		if p.NodeName == "n2" {
			victimUID = p.UID
		}
	}
	if victimUID == "" {
		t.Fatal("no pod landed on n2")
	}

	if err := h.mgr.MarkNodeNotReady(h.ctx, "n2"); err != nil {
		t.Fatal(err)
	}
	// Inside grace: no eviction, instance count unchanged.
	rep := h.reconcile("inside grace")
	if findEvent(rep, EvEvictNodeLost) != nil {
		t.Fatal("evicted inside grace window")
	}
	if rep.ActivePerRS["web"] != 2 {
		t.Fatalf("active changed inside grace: %d", rep.ActivePerRS["web"])
	}

	// Past grace: evict + reschedule onto n1, slot identity retained.
	h.clk.advance(31 * time.Second)
	if err := h.mgr.HeartbeatAt(h.ctx, "n1", h.clk.now()); err != nil {
		t.Fatal(err)
	}
	rep = h.reconcile("after grace")
	ev := findEvent(rep, EvEvictNodeLost)
	if ev == nil || ev.PodUID != victimUID {
		t.Fatalf("expected eviction of %s, got %+v", victimUID, ev)
	}
	if rep.Counts.Running != 2 {
		t.Fatalf("running = %d, want 2", rep.Counts.Running)
	}
	// New pod must occupy the SAME slot as the evicted one.
	replacementFound := false
	for _, p := range h.activePods("web") {
		if p.Slot == ev.Slot && p.NodeName == "n1" {
			replacementFound = true
		}
	}
	if !replacementFound {
		t.Fatalf("no replacement for slot %d on n1; pods=%+v", ev.Slot, h.activePods("web"))
	}
	// Every slot [0,2) still has exactly one active pod.
	seen := map[int]int{}
	for _, p := range h.activePods("web") {
		seen[p.Slot]++
	}
	for slot := 0; slot < 2; slot++ {
		if seen[slot] != 1 {
			t.Fatalf("slot %d occupancy = %d, want 1", slot, seen[slot])
		}
	}
}

func TestReconciler_FlutterWithinGraceCausesNoEviction(t *testing.T) {
	h := newHarness(t)
	h.addNode("n1", 100, 100)
	h.addNode("n2", 100, 100)
	h.applyRS(webRS(2, api.Resources{CPU: 10, Memory: 10}))
	h.reconcile("initial")

	if err := h.mgr.MarkNodeNotReady(h.ctx, "n2"); err != nil {
		t.Fatal(err)
	}
	h.clk.advance(10 * time.Second)
	rep := h.reconcile("10s flutter")
	if findEvent(rep, EvEvictNodeLost) != nil {
		t.Fatal("flutter evicted a pod")
	}

	// Recovery before grace: node back Ready, bindings untouched.
	if err := h.mgr.HeartbeatAt(h.ctx, "n2", h.clk.now()); err != nil {
		t.Fatal(err)
	}
	rep = h.reconcile("recovered")
	if findEvent(rep, EvEvictNodeLost) != nil || findEvent(rep, EvSchedule) != nil {
		t.Fatalf("recovery should not migrate pods: events=%+v", rep.Events)
	}
	for _, p := range h.activePods("web") {
		if p.NodeName == "" {
			t.Fatal("unexpected unbound pod after flutter recovery")
		}
	}
}

func TestReconciler_InstanceTotalsAlwaysExplainable(t *testing.T) {
	h := newHarness(t)
	h.addNode("n1", 100, 100)
	h.addNode("n2", 100, 100)
	h.applyRS(webRS(2, api.Resources{CPU: 10, Memory: 10}))
	h.reconcile("initial")

	if err := h.mgr.MarkNodeNotReady(h.ctx, "n2"); err != nil {
		t.Fatal(err)
	}
	h.clk.advance(31 * time.Second)
	if err := h.mgr.HeartbeatAt(h.ctx, "n1", h.clk.now()); err != nil {
		t.Fatal(err)
	}
	rep := h.reconcile("failover pass")
	// At the post-pass observation point: 1 old failed + 2 running instances
	// across the two slots => active == desired, and failed history explains
	// the one that was removed.
	if rep.ActivePerRS["web"] != 2 {
		t.Fatalf("active instances = %d, want desired 2", rep.ActivePerRS["web"])
	}
	if rep.Counts.Running != 2 || rep.Counts.Failed != 1 {
		t.Fatalf("counts = %+v, want running=2 failed=1", rep.Counts)
	}
}

func TestReconciler_ConcurrentPassesDoNotDuplicate(t *testing.T) {
	h := newHarness(t)
	for _, n := range []string{"n1", "n2", "n3"} {
		h.addNode(n, 1000, 1000)
	}
	h.applyRS(webRS(6, api.Resources{CPU: 10, Memory: 10}))

	const goroutines = 16
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := h.rec.ReconcileOnce(h.ctx, "")
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent reconcile: %v", err)
		}
	}

	active := h.activePods("web")
	if len(active) != 6 {
		t.Fatalf("active pods after concurrent reconciles = %d, want 6", len(active))
	}
	slots := map[int]int{}
	uids := map[string]int{}
	for _, p := range active {
		slots[p.Slot]++
		uids[p.UID]++
	}
	for slot := 0; slot < 6; slot++ {
		if slots[slot] != 1 {
			t.Fatalf("slot %d occupancy = %d, want 1", slot, slots[slot])
		}
	}
	for uid, n := range uids {
		if n != 1 {
			t.Fatalf("uid %s appears %d times", uid, n)
		}
	}
}

func TestReconciler_RestartResumesFromDisk(t *testing.T) {
	h := newHarness(t)
	h.addNode("n1", 1000, 1000)
	h.addNode("n2", 1000, 1000)
	h.applyRS(webRS(3, api.Resources{CPU: 10, Memory: 10}))
	h.reconcile("before restart")
	before := h.activePods("web")
	genBefore := h.st.Gen() // after the reconcile commit

	// Simulate process restart: reopen the same JSON file.
	h2 := restartHarness(t, h)
	rep := h2.reconcile("after restart")
	if len(rep.Events) != 0 {
		t.Fatalf("post-restart reconcile changed state: %+v", rep.Events)
	}
	after := h2.activePods("web")
	if len(after) != len(before) {
		t.Fatalf("active count after restart = %d, want %d", len(after), len(before))
	}
	if h2.st.Gen() != genBefore {
		t.Fatalf("gen after restart = %d, want %d", h2.st.Gen(), genBefore)
	}
	// Pod UIDs and node bindings must be identical, not rebuilt from scratch.
	beforeBySlot := map[int]api.Pod{}
	for _, p := range before {
		beforeBySlot[p.Slot] = p
	}
	for _, p := range after {
		b, ok := beforeBySlot[p.Slot]
		if !ok || b.UID != p.UID || b.NodeName != p.NodeName {
			t.Fatalf("slot %d changed across restart: before=%+v after=%+v", p.Slot, b, p)
		}
	}
}

func TestReconciler_NoExecuteTaintEvictsAndReschedules(t *testing.T) {
	h := newHarness(t)
	h.addNode("n1", 100, 100)
	h.addNode("n2", 100, 100)
	h.applyRS(webRS(1, api.Resources{CPU: 10, Memory: 10}))
	h.reconcile("initial")

	// Pod lands on n1 (name ascending tie-break).
	active := h.activePods("web")
	if len(active) != 1 || active[0].NodeName != "n1" {
		t.Fatalf("setup: expected 1 pod on n1, got %+v", active)
	}
	victim := active[0].UID
	slot := active[0].Slot

	// Drain n1 via an untolerated NoExecute taint.
	if err := h.mgr.AddTaint(h.ctx, "n1", api.Taint{Key: "drain", Value: "true", Effect: api.TaintEffectNoExecute}); err != nil {
		t.Fatal(err)
	}
	rep := h.reconcile("taint drain")
	ev := findEvent(rep, EvEvictTaint)
	if ev == nil || ev.PodUID != victim {
		t.Fatalf("expected NoExecute eviction of %s, got %+v", victim, rep.Events)
	}
	var replacement *api.Pod
	for _, p := range h.activePods("web") {
		if p.Slot == slot {
			replacement = &p
		}
	}
	if replacement == nil || replacement.NodeName != "n2" {
		t.Fatalf("expected replacement on n2, got %+v", replacement)
	}

	// A tolerating workload must NOT be evicted by the same taint.
	h.applyRS(api.ReplicaSet{Name: "tolerant", Namespace: "default", Replicas: 1,
		Template: api.PodSpec{
			NamePrefix: "tol", Request: api.Resources{CPU: 10, Memory: 10},
			Tolerations: []api.Toleration{{Key: "drain", Value: "true", Effect: api.TaintEffectNoExecute}},
		}})
	rep2 := h.reconcile("tolerant workload scheduled, web untouched")
	// web pod stays on n2 and tolerant pod may land on n1.
	for _, e := range rep2.Events {
		if e.RS == "web" && e.Kind == EvEvictTaint {
			t.Fatalf("web pod wrongly evicted after tolerant rs: %+v", e)
		}
	}
}

func TestReconciler_FailoverWhenNoCapacityKeepsOnePending(t *testing.T) {
	h := newHarness(t)
	// Each node fits exactly one pod; after n2 is lost there is no room for
	// the replacement, so it must stay Pending and never duplicate.
	h.addNode("n1", 10, 10)
	h.addNode("n2", 10, 10)
	h.applyRS(webRS(2, api.Resources{CPU: 10, Memory: 10}))
	h.reconcile("initial one per node")

	if err := h.mgr.MarkNodeNotReady(h.ctx, "n2"); err != nil {
		t.Fatal(err)
	}
	h.clk.advance(31 * time.Second)
	if err := h.mgr.HeartbeatAt(h.ctx, "n1", h.clk.now()); err != nil {
		t.Fatal(err)
	}
	rep := h.reconcile("n2 lost, no spare capacity")
	if rep.Counts.Running != 1 || rep.Counts.Pending != 1 {
		t.Fatalf("want 1 running + 1 pending, got %+v", rep.Counts)
	}
	if rep.ActivePerRS["web"] != 2 {
		t.Fatalf("active = %d, want 2 (1 running + 1 pending)", rep.ActivePerRS["web"])
	}
	// Multiple retries while still blocked must not accumulate pods.
	for i := 0; i < 3; i++ {
		h.clk.advance(5 * time.Second)
		if err := h.mgr.HeartbeatAt(h.ctx, "n1", h.clk.now()); err != nil {
			t.Fatal(err)
		}
		h.reconcile("blocked retry")
	}
	active := h.activePods("web")
	if len(active) != 2 {
		t.Fatalf("active grew to %d, want 2", len(active))
	}
	// Capacity appears -> the existing pending pod gets scheduled, no new pod.
	h.addNode("n3", 10, 10)
	rep2 := h.reconcile("n3 added")
	if rep2.Counts.Running != 2 || rep2.Counts.Pending != 0 {
		t.Fatalf("want 2 running, got %+v", rep2.Counts)
	}
	if len(h.activePods("web")) != 2 {
		t.Fatalf("active changed after scheduling pending: %d", len(h.activePods("web")))
	}
}

func TestReconciler_NodeDisappearedReschedules(t *testing.T) {
	h := newHarness(t)
	h.addNode("n1", 100, 100)
	h.addNode("n2", 100, 100)
	h.applyRS(webRS(2, api.Resources{CPU: 10, Memory: 10}))
	h.reconcile("initial")

	var victim string
	for _, p := range h.activePods("web") {
		if p.NodeName == "n2" {
			victim = p.UID
		}
	}
	if err := h.mgr.RemoveNode(h.ctx, "n2"); err != nil {
		t.Fatal(err)
	}
	rep := h.reconcile("node removed")
	ev := findEvent(rep, EvEvictNodeLost)
	if ev == nil || ev.PodUID != victim {
		t.Fatalf("expected eviction of %s after node removal", victim)
	}
	if rep.Counts.Running != 2 {
		t.Fatalf("running = %d, want 2", rep.Counts.Running)
	}
	for _, p := range h.activePods("web") {
		if p.NodeName != "n1" {
			t.Fatalf("expected all pods on n1, got %s on %s", p.UID, p.NodeName)
		}
	}
}
