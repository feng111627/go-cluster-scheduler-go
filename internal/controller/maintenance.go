package controller

import (
	"fmt"
	"sort"
	"time"

	"github.com/feng111627/go-cluster-scheduler-go/internal/cluster"
	"github.com/feng111627/go-cluster-scheduler-go/internal/scheduler"
)

// DrainResult summarizes one EnterMaintenance pass for explainability/tests.
type DrainResult struct {
	Node string `json:"node"`
	// Drained: pods moved Running -> Rescheduling off the node in THIS pass.
	Drained []string `json:"drained"`
	// Migrated: drained pods immediately re-bound to healthy nodes in the
	// same transaction (no extra reconcile trigger needed).
	Migrated []string `json:"migrated"`
	// Waiting: drained pods that no healthy node can fit right now; they stay
	// Rescheduling with WaitReason=MaintenanceDrain until capacity appears.
	Waiting []string `json:"waiting"`
	// WaitErrors holds the per-pod scheduling failure (with per-node reasons)
	// for every pod in Waiting, so the cause is distinguishable.
	WaitErrors []*scheduler.ScheduleError `json:"waitErrors"`
}

// EnterMaintenance marks a node as under planned maintenance, drains it and
// migrates the drained pods — all inside ONE serialized store transaction:
//
//  1. Node.Maintenance is set (persisted, so a restart keeps the node out of
//     scheduling) and MaintenanceSince is pinned on first entry;
//  2. every Running pod on the node is moved to Rescheduling (never deleted),
//     its OriginNode recorded, WaitReason pinned to MaintenanceDrain and its
//     resources released — the same "move, don't lose" semantics as failure
//     eviction, but with a distinguishable, restart-surviving wait reason;
//  3. each drained pod is immediately re-placed via the scheduler's
//     deterministic rule, still inside this transaction: instances migrate
//     onto healthy nodes with spare capacity RIGHT AWAY instead of parking
//     in a waiting state until someone triggers an extra reconcile. Pods
//     that fit nowhere stay Rescheduling (with their per-node rejection
//     reasons reported in the result) — never dropped, never forced onto
//     the maintenance node;
//  4. from this commit on the scheduler rejects the node with
//     ReasonNodeMaintenance, so no new instance can land on it.
//
// The whole operation is idempotent: re-entering maintenance (e.g. after a
// process restart that already persisted the flag) finds no Running pods
// left and only re-asserts the flag — progress continues, it never restarts
// and never migrates anything twice. At every instant the instance total
// and each instance's node assignment remain fully explainable.
func (c *Controller) EnterMaintenance(nodeName string) (*DrainResult, error) {
	res := &DrainResult{Node: nodeName}
	err := c.store.Update(func(st *cluster.State) error {
		n := st.Nodes[nodeName]
		if n == nil {
			return fmt.Errorf("unknown node: %s", nodeName)
		}
		if !n.Maintenance {
			n.Maintenance = true
			n.MaintenanceSince = c.now().UTC()
			c.logger.Printf("maintenance input: node=%s ENTER maintenance since=%s (persisted; scheduler now rejects with NodeMaintenance)",
				nodeName, n.MaintenanceSince.Format(time.RFC3339))
		} else {
			c.logger.Printf("maintenance input: node=%s already in maintenance since=%s -> re-asserting drain (idempotent)",
				nodeName, n.MaintenanceSince.Format(time.RFC3339))
		}

		// Drain in deterministic UID order: Running -> Rescheduling, resources
		// released. Pods are never deleted, so the instance total stays
		// explainable at every instant.
		uids := make([]string, 0, len(st.Pods))
		for uid, p := range st.Pods {
			if p.NodeName == nodeName && p.Phase == cluster.PhaseRunning {
				uids = append(uids, uid)
			}
		}
		sort.Strings(uids)
		for _, uid := range uids {
			p := st.Pods[uid]
			n.Used = n.Used.Sub(p.Requests)
			p.OriginNode = nodeName
			p.NodeName = ""
			p.Phase = cluster.PhaseRescheduling
			p.WaitReason = cluster.WaitReasonMaintenanceDrain
			p.Generation++
			res.Drained = append(res.Drained, uid)
		}
		c.logger.Printf("maintenance decision: node=%s DRAINED pods=%v (Running->Rescheduling waitReason=%s; resources released)",
			nodeName, res.Drained, cluster.WaitReasonMaintenanceDrain)

		// Migrate immediately, still inside this transaction: each drained
		// pod is placed onto a healthy node with spare capacity by the
		// scheduler's deterministic rule. Pods that fit nowhere keep waiting
		// with their distinguishable reasons — the caller does NOT need to
		// trigger an extra reconcile for the migration to happen.
		for _, uid := range res.Drained {
			perr := c.scheduler.PlaceInState(st, uid)
			if perr == nil {
				res.Migrated = append(res.Migrated, uid)
				continue
			}
			if se, ok := perr.(*scheduler.ScheduleError); ok {
				res.Waiting = append(res.Waiting, uid)
				res.WaitErrors = append(res.WaitErrors, se)
				c.logger.Printf("maintenance decision: node=%s pod=%s cannot migrate yet aggregate=%s -> stays Rescheduling (waitReason=%s)",
					nodeName, uid, se.AggregateReason(), cluster.WaitReasonMaintenanceDrain)
				continue
			}
			return perr // abort the whole transaction on unexpected errors
		}
		c.logger.Printf("maintenance decision: node=%s drain complete drained=%d migrated=%d waiting=%v",
			nodeName, len(res.Drained), len(res.Migrated), res.Waiting)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ExitMaintenance lets a maintained node participate in scheduling again —
// but ONLY to the extent the node has verifiably recovered:
//
//   - the persisted Maintenance flag is always cleared (operator intent);
//   - if the node's heartbeat is FRESH (within HeartbeatTimeout), the node
//     is genuinely back: it becomes Ready and schedulable immediately, with
//     UnreachableSince/Evicted reset so the failure detector does not pounce
//     on stale history accumulated during maintenance;
//   - if the node is still SILENT (no fresh heartbeat), it must NOT be
//     treated as healthy: it stays NotReady, so the scheduler keeps
//     rejecting it with NodeNotReady and no instance can be placed on a
//     node that the failure detector would evict moments later (which is
//     exactly the "placed, then evicted, back and forth" flap this
//     prevents). The node re-enters normal failure-detection jurisdiction
//     with a fresh grace window starting now: if it actually re-reports,
//     DetectNodeFailures flips it Ready (zero churn); if it stays silent
//     past the grace period, the normal eviction path applies.
//
// Anti-flap contract: recovery NEVER migrates Running pods back onto this
// node. Only future unbound pods (new replicas, later evictions) may be
// placed here, via the same deterministic scheduler rule as everywhere else.
// That makes rebalancing stable and predictable: a running instance is never
// moved by recovery, so "move back, then move away again" cannot happen.
//
// The operation is idempotent: exiting a node that is not in maintenance is
// a no-op.
func (c *Controller) ExitMaintenance(nodeName string) error {
	return c.store.Update(func(st *cluster.State) error {
		n := st.Nodes[nodeName]
		if n == nil {
			return fmt.Errorf("unknown node: %s", nodeName)
		}
		if !n.Maintenance {
			c.logger.Printf("maintenance input: node=%s not in maintenance -> exit is a no-op (idempotent)", nodeName)
			return nil
		}
		n.Maintenance = false
		n.MaintenanceSince = time.Time{}

		now := c.now().UTC()
		if now.Sub(n.LastSeen) <= c.heartbeatTimeout {
			// Verifiably alive: rejoin scheduling immediately on a clean
			// baseline, so the failure detector sees a healthy node.
			n.Ready = true
			n.UnreachableSince = time.Time{}
			n.Evicted = false
			c.logger.Printf("maintenance decision: node=%s EXIT maintenance -> heartbeat fresh (lastSeen=%s), Ready and schedulable again; running pods are NOT migrated back (anti-flap)",
				nodeName, n.LastSeen.Format(time.RFC3339))
			return nil
		}

		// Still silent: do NOT treat the node as healthy. It stays NotReady
		// (scheduler rejects it) and re-enters failure detection with a fresh
		// grace window from now; only a real heartbeat will make it Ready.
		n.Ready = false
		n.UnreachableSince = now
		c.logger.Printf("maintenance decision: node=%s EXIT maintenance but heartbeat stale (lastSeen=%s) -> stays NotReady until it actually re-reports (no place-then-evict flap)",
			nodeName, n.LastSeen.Format(time.RFC3339))
		return nil
	})
}
