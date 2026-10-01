// Command scheduler runs a small in-process demonstration of the local
// cluster scheduler + reconciler: scheduling, scale in/out, node failover
// with grace period, and state recovery after a simulated process restart.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/feng111627/go-cluster-scheduler-go/api"
	"github.com/feng111627/go-cluster-scheduler-go/controller"
	"github.com/feng111627/go-cluster-scheduler-go/scheduler"
	"github.com/feng111627/go-cluster-scheduler-go/store"
)

func main() {
	const statePath = ".data/cluster-state.json"
	_ = os.MkdirAll(filepath.Dir(statePath), 0o755)

	logger := log.New(os.Stdout, "[demo] ", log.LstdFlags|log.Lmicroseconds)
	ctx := context.Background()

	// start returns a fresh process-like handle over the same state file.
	start := func() (*store.Store, *controller.Manager, *controller.Reconciler, *fakeClock) {
		st, err := store.New(ctx, statePath)
		if err != nil {
			log.Fatalf("open store: %v", err)
		}
		clk := &fakeClock{t: time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)}
		sch := scheduler.New(logger)
		cfg := controller.Config{
			HeartbeatTimeout: 30 * time.Second,
			NodeGracePeriod:  30 * time.Second,
			RequeueInterval:  time.Second,
		}
		mgr := controller.NewManager(st, clk.Now)
		rec := controller.New(st, sch, cfg, logger, clk.Now)
		return st, mgr, rec, clk
	}

	// ---- "first boot": seed the cluster ------------------------------------
	st, mgr, rec, clk := start()
	if st.Fresh() {
		logger.Printf("== first boot: seed 3 nodes and a 3-replica workload ==")
		for _, name := range []string{"node-1", "node-2", "node-3"} {
			must(mgr.AddNode(ctx, api.Node{Name: name, Capacity: api.Resources{CPU: 10_000, Memory: 10 << 30}}))
		}
		must(mgr.ApplyReplicaSet(ctx, api.ReplicaSet{
			Name: "web", Namespace: "default", Replicas: 3,
			Template: api.PodSpec{
				NamePrefix: "web",
				Request:    api.Resources{CPU: 2_000, Memory: 1 << 30},
			},
		}))
	} else {
		logger.Printf("== resumed from persisted state, generation=%d ==", st.Gen())
	}

	run := func(label string) {
		rep, err := rec.ReconcileOnce(ctx, "")
		must(err)
		printState(label, mgr, rep)
	}
	// refreshBeats simulates healthy kubelets reporting in at the clock's
	// current time; call it after advancing the clock (and before reconcile).
	refreshBeats := func() {
		for _, n := range mgr.ListNodes() {
			if n.Phase == api.NodeReady {
				must(mgr.HeartbeatAt(ctx, n.Name, clk.Now()))
			}
		}
	}

	refreshBeats()
	run("pass 1: initial scheduling")

	// ---- scale out ----------------------------------------------------------
	logger.Printf("== scale web: 3 -> 4 replicas ==")
	must(mgr.ApplyReplicaSet(ctx, api.ReplicaSet{
		Name: "web", Namespace: "default", Replicas: 4,
		Template: api.PodSpec{NamePrefix: "web", Request: api.Resources{CPU: 2_000, Memory: 1 << 30}},
	}))
	refreshBeats()
	run("pass 2: scale out")

	// Repeated reconcile must be a no-op (idempotence).
	refreshBeats()
	run("pass 3: repeated reconcile (expect no changes)")

	// ---- node failure: graceful failover -----------------------------------
	logger.Printf("== node-2 stops heartbeating ==")
	must(mgr.MarkNodeNotReady(ctx, "node-2"))
	run("pass 4: node-2 NotReady, inside grace (expect no eviction)")

	clk.Advance(35 * time.Second) // beyond 30s heartbeat timeout AND grace
	refreshBeats()                // node-1/node-3 keep reporting; node-2 stays silent
	run("pass 5: grace elapsed, failover to healthy node")
	refreshBeats()
	run("pass 6: repeated reconcile after failover (expect no duplicates)")

	// ---- short flutter must not cause eviction ------------------------------
	logger.Printf("== node-3 has a 10s heartbeat flutter ==")
	must(mgr.MarkNodeNotReady(ctx, "node-3"))
	clk.Advance(10 * time.Second)
	refreshBeats() // node-1 stays alive; node-3 is silent for 10s
	run("pass 7: node-3 flutter inside grace")
	// Node-3 recovers within the grace window: no migration should happen.
	must(mgr.HeartbeatAt(ctx, "node-3", clk.Now()))
	run("pass 8: node-3 heartbeat recovered (expect no migration)")

	// ---- simulated restart ---------------------------------------------------
	logger.Printf("== simulating process restart, state file: %s ==", statePath)
	st.Close()
	st2, mgr2, rec2, clk2 := start()
	defer st2.Close()
	clk2.t = clk.t
	logger.Printf("restarted: generation=%d", st2.Gen())
	rep, err := rec2.ReconcileOnce(ctx, "")
	must(err)
	printState("pass 9: after restart (same pods, same nodes)", mgr2, rep)
}

func printState(label string, mgr *controller.Manager, rep *controller.Report) {
	fmt.Printf("--- %s (gen=%d) ---\n", label, rep.Gen)
	for _, e := range rep.Events {
		fmt.Printf("  event: %-20s rs=%-4s slot=%d pod=%s node=%s %s %s\n",
			e.Kind, e.RS, e.Slot, e.PodUID, e.Node, e.Reason, e.Detail)
	}
	for _, n := range mgr.ListNodes() {
		fmt.Printf("  node: %-7s phase=%-9s usedCpu=%5d usedMem=%d\n",
			n.Name, n.Phase, n.Used.CPU, n.Used.Memory)
	}
	for _, p := range mgr.ListPods() {
		fmt.Printf("  pod : %-12s slot=%d phase=%-8s node=%-7s %s\n",
			p.UID, p.Slot, p.Phase, p.NodeName, p.Message)
	}
	fmt.Printf("  totals: running=%d pending=%d failed=%d activePerRS=%v\n\n",
		rep.Counts.Running, rep.Counts.Pending, rep.Counts.Failed, rep.ActivePerRS)
}

func must(err error) {
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
}
