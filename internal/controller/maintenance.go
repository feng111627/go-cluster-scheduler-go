package controller

import (
	"fmt"
	"sort"
	"time"

	"github.com/feng111627/go-cluster-scheduler-go/internal/cluster"
)

// EnterMaintenance marks a node as under planned maintenance and drains it
// inside ONE serialized store transaction:
//
//  1. Node.Maintenance is set (persisted, so a restart keeps the node out of
//     scheduling) and MaintenanceSince is pinned on first entry;
//  2. every Running pod on the node is moved to Rescheduling (never deleted),
//     its OriginNode recorded and its resources released — the same
//     "move, don't lose" semantics as failure eviction;
//  3. from this commit on the scheduler rejects the node with
//     ReasonNodeMaintenance, so no new instance can land on it.
//
// The whole operation is idempotent: re-entering maintenance (e.g. after a
// process restart that already persisted the flag) finds no Running pods left
// and only re-asserts the flag. The returned UIDs are the pods drained in
// THIS pass; the caller reconciles the owning deployments to re-place them.
func (c *Controller) EnterMaintenance(nodeName string) ([]string, error) {
	var drained []string
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
			p.Generation++
			drained = append(drained, uid)
		}
		c.logger.Printf("maintenance decision: node=%s DRAINED pods=%v (Running->Rescheduling; resources released; awaiting reschedule onto healthy nodes)",
			nodeName, drained)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return drained, nil
}

// ExitMaintenance brings a maintained node back into service inside ONE
// serialized transaction:
//
//   - the persisted Maintenance flag is cleared, so the node becomes a
//     scheduling candidate again;
//   - Ready/LastSeen/Evicted/UnreachableSince are reset to a fresh, healthy
//     baseline, so the failure detector does not immediately pounce on the
//     stale heartbeat history accumulated during maintenance.
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
		n.Ready = true
		n.LastSeen = c.now().UTC()
		n.UnreachableSince = time.Time{}
		n.Evicted = false
		c.logger.Printf("maintenance decision: node=%s EXIT maintenance -> Ready, schedulable again; running pods are NOT migrated back (anti-flap)",
			nodeName)
		return nil
	})
}
