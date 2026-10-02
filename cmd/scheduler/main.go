// Command scheduler is a local, file-backed demo of the cluster pod scheduler
// and reconciliation controller. It uses no external services: cluster state
// is simulated in memory and persisted to a local JSON file.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/feng111627/go-cluster-scheduler-go/internal/cluster"
	"github.com/feng111627/go-cluster-scheduler-go/internal/controller"
	"github.com/feng111627/go-cluster-scheduler-go/internal/scheduler"
)

func main() {
	stateFile := flag.String("state-file", "cluster-state.json", "path to the durable state file")
	scenario := flag.String("scenario", "default", "demo scenario: default (scheduling/failover) or maintenance (planned drain)")
	flag.Parse()

	logger := log.New(os.Stdout, "[scheduler] ", log.LstdFlags|log.Lmicroseconds)

	if *scenario == "maintenance" {
		// The maintenance demo is self-contained and uses its own state file by
		// default, so it never collides with the default-scenario artifacts.
		file := *stateFile
		if *stateFile == "cluster-state.json" {
			file = "cluster-maintenance-state.json"
		}
		moveAsideIfExists(file, logger)
		if err := runMaintenanceDemo(file, logger); err != nil {
			logger.Fatalf("maintenance demo failed: %v", err)
		}
		return
	}

	// A demo only makes sense from an empty state; move the old file aside.
	moveAsideIfExists(*stateFile, logger)

	if err := run(*stateFile, logger); err != nil {
		logger.Fatalf("demo failed: %v", err)
	}
}

func moveAsideIfExists(file string, logger *log.Logger) {
	if _, err := os.Stat(file); err == nil {
		backup := file + ".bak"
		if err := os.Rename(file, backup); err == nil {
			logger.Printf("existing state moved aside: %s", backup)
		}
	}
}

// clock is a controllable virtual clock shared by store and controller so the
// failure-detection timeline is fully deterministic.
type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }
func (c *clock) advance(d time.Duration) time.Time {
	c.t = c.t.Add(d)
	return c.t
}

func run(stateFile string, logger *log.Logger) error {
	clk := &clock{t: time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)}

	st, err := cluster.NewStore(stateFile)
	if err != nil {
		return fmt.Errorf("load state: %w", err)
	}
	sch := scheduler.New(st, logger)
	cfg := controller.Config{HeartbeatTimeout: 10 * time.Second, EvictionGrace: 30 * time.Second}
	ctl := controller.NewWithClock(st, sch, cfg, logger, clk.now)

	mCPU := func(v int64) cluster.Quantity { return cluster.Quantity(v) }
	// Three nodes: a big one, a small one, a tainted one.
	nodes := []*cluster.Node{
		{Name: "node-a", Capacity: cluster.Resources{CPU: mCPU(4000), Memory: 8192}, Ready: true, LastSeen: clk.now()},
		{Name: "node-b", Capacity: cluster.Resources{CPU: mCPU(2000), Memory: 4096}, Ready: true, LastSeen: clk.now()},
		{Name: "node-c", Capacity: cluster.Resources{CPU: mCPU(4000), Memory: 8192}, Ready: true, LastSeen: clk.now(),
			Taints: []cluster.Taint{{Key: "gpu", Value: "true", Effect: cluster.TaintNoSchedule}}},
	}
	for _, n := range nodes {
		if err := st.UpsertNode(n); err != nil {
			return err
		}
	}

	dep := &cluster.Deployment{
		Name:      "web",
		Namespace: "default",
		Replicas:  4,
		Template: cluster.PodTemplate{
			NamePrefix: "web",
			Requests:   cluster.Resources{CPU: mCPU(1000), Memory: 1024},
		},
	}
	if err := st.UpsertDeployment(dep); err != nil {
		return err
	}

	// 1) Reconcile: 4 replicas created and placed deterministically.
	logger.Printf("==== STEP 1: reconcile web to 4 replicas ====")
	if err := reconcile(ctl, "web"); err != nil {
		return err
	}
	printCounts(logger, st)

	// 2) Repeat reconcile concurrently: must be a no-op (no duplicates).
	logger.Printf("==== STEP 2: 5 concurrent repeated reconciles (idempotency) ====")
	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- reconcile(ctl, "web") }()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			return e
		}
	}
	printCounts(logger, st)

	// 3) node-a loses heartbeats: at first only Unreachable (grace), then evicted.
	// Healthy nodes keep heartbeating as the virtual clock advances.
	logger.Printf("==== STEP 3: node-a goes silent ====")
	clk.advance(15 * time.Second) // past heartbeat timeout, within eviction grace
	healthyHeartbeat(st, clk, "node-b", "node-c")
	if _, err := ctl.DetectNodeFailures(); err != nil {
		return err
	}
	printCounts(logger, st)

	// A reconcile during the grace window does not over-create: total stays 4.
	logger.Printf("==== STEP 4: reconcile while node-a is merely unreachable (no churn) ====")
	if err := reconcile(ctl, "web"); err != nil {
		return err
	}
	printCounts(logger, st)

	logger.Printf("==== STEP 5: node-a stays silent past grace -> eviction + reschedule ====")
	clk.advance(25 * time.Second) // 40s total since silence: beyond grace
	healthyHeartbeat(st, clk, "node-b", "node-c")
	evicted, err := ctl.DetectNodeFailures()
	if err != nil {
		return err
	}
	logger.Printf("evicted pods: %v", evicted)
	if err := reconcile(ctl, "web"); err != nil {
		return err
	}
	printCounts(logger, st)

	// 4) Resource insufficiency demo: a tolerated, oversized workload fills
	// node-b + node-c and then reports InsufficientCPU.
	logger.Printf("==== STEP 6: add tolerated workload 'batch' with 20 replicas to observe InsufficientCPU ====")
	batch := &cluster.Deployment{
		Name:      "batch",
		Namespace: "default",
		Replicas:  20,
		Template: cluster.PodTemplate{
			NamePrefix: "batch",
			Requests:   cluster.Resources{CPU: mCPU(1000), Memory: 512},
			Tolerations: []cluster.Toleration{
				{Key: "gpu", Operator: "Equal", Value: "true", Effect: "NoSchedule"},
			},
		},
	}
	if err := st.UpsertDeployment(batch); err != nil {
		return err
	}
	err = reconcileRaw(ctl, "batch")
	var rerr *controller.ReconcileError
	if errors.As(err, &rerr) {
		reasons := map[scheduler.Reason]int{}
		for _, se := range rerr.ScheduleErrors {
			reasons[se.AggregateReason()]++
		}
		logger.Printf("expected unschedulable batch pods, reason histogram: %v", reasons)
	} else if err != nil {
		return err
	}
	printCounts(logger, st)

	// 5) Restart: reload state from disk in a fresh store/controller.
	logger.Printf("==== STEP 7: simulate process restart from %s ====", stateFile)
	st2, err := cluster.NewStore(stateFile)
	if err != nil {
		return err
	}
	sch2 := scheduler.New(st2, logger)
	ctl2 := controller.NewWithClock(st2, sch2, cfg, logger, clk.now)
	for _, d := range []string{"web", "batch"} {
		if err := reconcile(ctl2, d); err != nil && !errors.As(err, &rerr) {
			return err
		}
	}
	printCounts(logger, st2)
	logger.Printf("restart recovery complete: state epoch continues from persisted value")
	return nil
}

// reconcile tolerates a ReconcileError (some pods unschedulable) but returns
// other errors immediately.
func reconcile(ctl *controller.Controller, name string) error {
	_, err := ctl.ReconcileDeployment(name)
	var rerr *controller.ReconcileError
	if errors.As(err, &rerr) {
		return nil // unschedulable replicas are an expected, reported outcome
	}
	return err
}

// reconcileRaw invokes one reconcile pass and returns its raw error.
func reconcileRaw(ctl *controller.Controller, name string) error {
	_, err := ctl.ReconcileDeployment(name)
	return err
}

// healthyHeartbeat simulates live nodes continuing to report heartbeats at
// the virtual clock's current time.
func healthyHeartbeat(st *cluster.Store, clk *clock, nodes ...string) {
	for _, n := range nodes {
		if err := st.HeartbeatAt(n, clk.now()); err != nil {
			panic(err)
		}
	}
}

func printCounts(logger *log.Logger, st *cluster.Store) {
	byPhase := st.CountPods()
	total := 0
	for _, n := range byPhase {
		total += n
	}
	logger.Printf("pod counts: total=%d running=%d pending=%d rescheduling=%d failed=%d",
		total, byPhase[cluster.PhaseRunning], byPhase[cluster.PhasePending],
		byPhase[cluster.PhaseRescheduling], byPhase[cluster.PhaseFailed])
}
