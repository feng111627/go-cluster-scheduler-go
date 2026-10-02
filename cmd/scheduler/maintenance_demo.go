package main

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/feng111627/go-cluster-scheduler-go/internal/cluster"
	"github.com/feng111627/go-cluster-scheduler-go/internal/controller"
	"github.com/feng111627/go-cluster-scheduler-go/internal/scheduler"
)

// runMaintenanceDemo walks a self-contained planned-maintenance lifecycle
// against the durable state file:
//
//  1. place 4 replicas on two healthy nodes;
//  2. cordon node-a (planned drain): running pods migrate to node-b, the ones
//     that do not fit WAIT with reason DrainingForMaintenance (nothing is
//     dropped or force-placed);
//  3. "process restart": reload the file mid-maintenance — the cordon still
//     holds, drain resumes without re-migrating;
//  4. maintenance ends: node-a re-enters the pool, waiters re-bind to it while
//     already-migrated pods stay put (no immediate move-back / churn);
//  5. a subsequent reconcile is a no-op (stable, predictable re-balance rule).
func runMaintenanceDemo(stateFile string, logger *log.Logger) error {
	clk := &clock{t: time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC)}

	st, err := cluster.NewStore(stateFile)
	if err != nil {
		return fmt.Errorf("load state: %w", err)
	}
	sch := scheduler.New(st, logger)
	cfg := controller.Config{HeartbeatTimeout: 10 * time.Second, EvictionGrace: 30 * time.Second}
	ctl := controller.NewWithClock(st, sch, cfg, logger, clk.now)

	mCPU := func(v int64) cluster.Quantity { return cluster.Quantity(v) }

	// node-a is big (hosts all 4 initially); node-b fits only 2, so the drain
	// must leave a visible, distinguishable waiting set.
	nodes := []*cluster.Node{
		{Name: "node-a", Capacity: cluster.Resources{CPU: mCPU(8000), Memory: 8192}, Ready: true, LastSeen: clk.now()},
		{Name: "node-b", Capacity: cluster.Resources{CPU: mCPU(2000), Memory: 4096}, Ready: true, LastSeen: clk.now()},
	}
	for _, n := range nodes {
		if err := st.UpsertNode(n); err != nil {
			return err
		}
	}
	dep := &cluster.Deployment{
		Name: "web", Namespace: "default", Replicas: 4,
		Template: cluster.PodTemplate{
			NamePrefix: "web",
			Requests:   cluster.Resources{CPU: mCPU(1000), Memory: 1024},
		},
	}
	if err := st.UpsertDeployment(dep); err != nil {
		return err
	}

	logger.Printf("==== MAINT STEP 1: reconcile web to 4 replicas ====")
	if err := reconcile(ctl, "web"); err != nil {
		return err
	}
	printPlacementDetails(logger, st)

	logger.Printf("==== MAINT STEP 2: cordon node-a (planned maintenance drain) ====")
	res, err := ctl.StartNodeMaintenance("node-a")
	if err != nil {
		return fmt.Errorf("start maintenance: %w", err)
	}
	logger.Printf("drain result: migrated=%v waiting=%v stillOnNode=%v flagAlreadySet=%t",
		res.Migrated, res.Waiting, res.StillRunning, res.FlagAlreadySet)
	printPlacementDetails(logger, st)

	logger.Printf("==== MAINT STEP 3: simulated process restart mid-maintenance ====")
	st2, err := cluster.NewStore(stateFile)
	if err != nil {
		return err
	}
	sch2 := scheduler.New(st2, logger)
	ctl2 := controller.NewWithClock(st2, sch2, cfg, logger, clk.now)
	var cordoned bool
	st2.View(func(s *cluster.State) { cordoned = s.Nodes["node-a"].UnderMaintenance })
	if !cordoned {
		return fmt.Errorf("maintenance flag was forgotten across restart")
	}
	res2, err := ctl2.StartNodeMaintenance("node-a") // resume, not restart
	if err != nil {
		return err
	}
	logger.Printf("resumed drain: migrated=%v waiting=%v (no double migration)", res2.Migrated, res2.Waiting)
	if err := reconcile(ctl2, "web"); err != nil && !errors.As(err, new(*controller.ReconcileError)) {
		return err
	}
	printPlacementDetails(logger, st2)

	logger.Printf("==== MAINT STEP 4: maintenance ends; node-a re-enters the pool ====")
	changed, err := ctl2.EndNodeMaintenance("node-a")
	if err != nil {
		return err
	}
	logger.Printf("uncordon changed=%t (running pods are NOT moved back; only waiters are re-eligible)", changed)
	if err := reconcile(ctl2, "web"); err != nil {
		return err
	}
	printPlacementDetails(logger, st2)

	logger.Printf("==== MAINT STEP 5: repeat reconcile must be a no-op (anti-flap, stable) ====")
	if err := reconcile(ctl2, "web"); err != nil {
		return err
	}
	printPlacementDetails(logger, st2)
	logger.Printf("maintenance lifecycle complete; durable state at %s", stateFile)
	return nil
}

// printPlacementDetails logs the total count and the exact pod -> node assignment.
func printPlacementDetails(logger *log.Logger, st *cluster.Store) {
	byPhase := st.CountPods()
	total := 0
	for _, n := range byPhase {
		total += n
	}
	type bind struct{ uid, node string }
	var binds []bind
	var waitReasons []string
	st.View(func(s *cluster.State) {
		for uid, p := range s.Pods {
			node := p.NodeName
			if node == "" {
				node = "-"
			}
			binds = append(binds, bind{uid, node})
			if p.WaitReason != "" {
				waitReasons = append(waitReasons, fmt.Sprintf("%s:%s", uid, p.WaitReason))
			}
		}
	})
	logger.Printf("pod counts: total=%d running=%d pending=%d rescheduling=%d failed=%d waiting=%v",
		total, byPhase[cluster.PhaseRunning], byPhase[cluster.PhasePending],
		byPhase[cluster.PhaseRescheduling], byPhase[cluster.PhaseFailed], waitReasons)
	for _, b := range binds {
		logger.Printf("  placement: %s -> %s", b.uid, b.node)
	}
}
