package controller

import (
	"fmt"
	"sort"
	"time"

	"github.com/feng111627/go-cluster-scheduler-go/internal/cluster"
	"github.com/feng111627/go-cluster-scheduler-go/internal/scheduler"
)

// MaintenanceResult summarizes one EnterMaintenance pass for explainability
// and tests. All UIDs are drained pods of THIS pass, split by outcome.
type MaintenanceResult struct {
	Drained  []string `json:"drained"`  // Running -> Rescheduling in this pass
	Migrated []string `json:"migrated"` // re-bound onto healthy nodes in the SAME pass
	Waiting  []string `json:"waiting"`  // still Rescheduling: no healthy node can fit them
	// WaitErrors carries the per-node rejection reasons for each Waiting pod,
	// so "healthy nodes full" stays distinguishable from "node in maintenance".
	WaitErrors []*scheduler.ScheduleError `json:"waitErrors,omitempty"`
}

// EnterMaintenance marks a node as under planned maintenance, drains it AND
// migrates the drained pods — all inside ONE serialized store transaction:
//
//  1. Node.Maintenance is set (persisted, so a restart keeps the node out of
//     scheduling) and MaintenanceSince is pinned on first entry;
//  2. every Running pod on the node is moved to Rescheduling (never deleted),
//     its OriginNode recorded, WaitReason=MaintenanceDrain stamped and its
//     resources released — the same "move, don't lose" semantics as failure
//     eviction, but with a distinguishable, persisted wait cause;
//  3. each drained pod is immediately re-placed via the deterministic
//     scheduler rule, in the SAME transaction — the caller does NOT need an
//     extra reconcile to make the migration actually happen;
//  4. pods no healthy node can fit stay Rescheduling with
//     WaitReason=MaintenanceDrain; their per-node ScheduleError reasons say
//     exactly why (maintenance node rejected vs. healthy nodes full).
//
// From this commit on the scheduler rejects the node with
// ReasonNodeMaintenance, so nothing can land back on it mid-drain.
//
// The whole operation is idempotent: re-entering maintenance (e.g. after a
// process restart that already persisted the flag) finds no Running pods left
// and only re-asserts the flag — already-migrated pods are never re-migrated.
func (c *Controller) EnterMaintenance(nodeName string) (*MaintenanceResult, error) {
	res := &MaintenanceResult{}
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
		// released, wait cause stamped. Pods are never deleted, so the
		// instance total stays explainable at every instant.
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
			p.WaitReason = cluster.WaitMaintenanceDrain
			p.NodeName = ""
			p.Phase = cluster.PhaseRescheduling
			p.Generation++
			res.Drained = append(res.Drained, uid)
		}
		c.logger.Printf("maintenance decision: node=%s DRAINED pods=%v (Running->Rescheduling waitReason=%s; resources released)",
			nodeName, res.Drained, cluster.WaitMaintenanceDrain)

		// Migrate immediately, still inside this transaction: a drained pod
		// must not sit waiting for an extra external trigger. Pods that no
		// healthy node can fit keep Waiting with their reasons recorded.
		for _, uid := range res.Drained {
			perr := c.scheduler.PlaceInState(st, uid)
			if perr == nil {
				res.Migrated = append(res.Migrated, uid)
				continue
			}
			if se, ok := perr.(*scheduler.ScheduleError); ok {
				res.Waiting = append(res.Waiting, uid)
				res.WaitErrors = append(res.WaitErrors, se)
				c.logger.Printf("maintenance decision: node=%s pod=%s cannot migrate yet aggregate=%s -> stays Rescheduling(waitReason=%s)",
					nodeName, uid, se.AggregateReason(), cluster.WaitMaintenanceDrain)
				continue
			}
			return perr // abort the whole maintenance op on unexpected errors
		}
		c.logger.Printf("maintenance result: node=%s drained=%d migrated=%d waiting=%v",
			nodeName, len(res.Drained), len(res.Migrated), res.Waiting)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ExitMaintenance asks a maintained node to rejoin service, inside ONE
// serialized transaction. It returns rejoined=true only when the node is
// genuinely healthy afterwards.
//
// Recovery gating (the anti ping-pong contract): the maintenance flag is
// always cleared, but the node only becomes a scheduling candidate when it
// has ACTUALLY recovered — i.e. its last heartbeat is still fresh
// (within heartbeatTimeout). A node that is still silent (never reported
// again after the operator asked it back) is NOT declared healthy:
//
//   - Ready stays false, so the scheduler keeps rejecting it with
//     NodeNotReady and no instance can be placed on a dead node;
//   - UnreachableSince is pinned (LastSeen + heartbeatTimeout) so the
//     failure detector takes over with its normal grace accounting — the
//     same path as any other lost node, no special-casing, no conflict with
//     flap protection;
//   - when the node finally heartbeats again, DetectNodeFailures restores it
//     to Ready through the regular recovery path.
//
// This kills the "placed onto a not-really-recovered node, then evicted as
// lost" ping-pong by construction: instances can only land on the node once
// liveness is proven, and the judgment is a pure function of the persisted
// heartbeat — stable and predictable.
//
// Anti-flap on placement: recovery NEVER migrates Running pods back onto
// this node. Only future unbound pods (new replicas, later evictions) may be
// placed here, via the same deterministic scheduler rule as everywhere else.
//
// The operation is idempotent: exiting a node that is not in maintenance is
// a no-op; rejoined then simply reports the node's current health.
func (c *Controller) ExitMaintenance(nodeName string) (bool, error) {
	rejoined := false
	err := c.store.Update(func(st *cluster.State) error {
		n := st.Nodes[nodeName]
		if n == nil {
			return fmt.Errorf("unknown node: %s", nodeName)
		}
		if !n.Maintenance {
			rejoined = n.Ready
			c.logger.Printf("maintenance input: node=%s not in maintenance -> exit is a no-op (idempotent; ready=%t)",
				nodeName, n.Ready)
			return nil
		}
		n.Maintenance = false
		n.MaintenanceSince = time.Time{}

		now := c.now().UTC()
		if now.Sub(n.LastSeen) <= c.heartbeatTimeout {
			// Genuinely alive: clear the failure-detector history accumulated
			// before/during maintenance and rejoin scheduling immediately.
			n.Ready = true
			n.UnreachableSince = time.Time{}
			n.Evicted = false
			rejoined = true
			c.logger.Printf("maintenance decision: node=%s EXIT maintenance, heartbeat fresh (lastSeen=%s) -> Ready, schedulable again; running pods are NOT migrated back (anti-flap)",
				nodeName, n.LastSeen.Format(time.RFC3339))
			return nil
		}

		// NOT actually recovered: keep the node out of scheduling and hand it
		// to the failure detector's normal accounting. It becomes Ready only
		// when a real heartbeat arrives (stable, predictable judgment).
		n.Ready = false
		if n.UnreachableSince.IsZero() {
			n.UnreachableSince = n.LastSeen.Add(c.heartbeatTimeout)
		}
		c.logger.Printf("maintenance decision: node=%s EXIT maintenance but heartbeat STALE (lastSeen=%s, silentFor=%s > timeout=%s) -> stays NotReady/unschedulable; failure detector owns it until a real heartbeat",
			nodeName, n.LastSeen.Format(time.RFC3339), now.Sub(n.LastSeen), c.heartbeatTimeout)
		return nil
	})
	if err != nil {
		return false, err
	}
	return rejoined, nil
}
