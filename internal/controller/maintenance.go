package controller

import (
	"fmt"
	"sort"

	"github.com/feng111627/go-cluster-scheduler-go/internal/cluster"
	"github.com/feng111627/go-cluster-scheduler-go/internal/scheduler"
)

// DrainResult summarizes one planned-maintenance (drain) pass.
type DrainResult struct {
	Node             string `json:"node"`
	UnderMaintenance bool   `json:"underMaintenance"`
	// Migrated: pods that were Running on the node and got re-bound elsewhere
	// within this same drain transaction.
	Migrated []string `json:"migrated"`
	// Rebound: previously waiting drain pods (e.g. after a restart, or once
	// capacity appeared) that this pass successfully bound to a healthy node.
	Rebound []string `json:"rebound"`
	// Waiting: pods that could not fit any other healthy node; they stay in
	// Rescheduling with WaitReason=DrainingForMaintenance and are retried by
	// later drain/reconcile passes (never dropped, never force-placed).
	Waiting []string `json:"waiting"`
	// StillRunning: Running pods left on the node after the pass. Must be zero
	// once the drain has converged.
	StillRunning []string `json:"stillRunning"`
	// FlagAlreadySet is true when the durable maintenance flag was already on
	// before this call (repeated/resumed drain). Such a call retries only the
	// node's still-waiting pods, so already-migrated pods are never moved twice.
	FlagAlreadySet bool `json:"flagAlreadySet"`
}

// ErrNodeNotFound identifies a maintenance operation on an unknown node.
type ErrNodeNotFound struct{ Node string }

func (e *ErrNodeNotFound) Error() string { return fmt.Sprintf("node not found: %s", e.Node) }

// StartNodeMaintenance marks a node for planned, graceful decommission and
// migrates its workload. It runs entirely inside ONE serialized store
// transaction, so concurrent maintenance/recovery/reconcile calls cannot
// interleave torn state:
//
//  1. the node's durable UnderMaintenance flag is set (survives restart; the
//     node stops receiving new placements via the scheduler gate immediately);
//  2. its Running pods are detached in deterministic UID order — resources are
//     released on the drained node — and re-placed through the SAME scheduler
//     the rest of the system uses, so target selection is deterministic and
//     consistent with normal scheduling;
//  3. pods that fit nowhere remain Rescheduling with the distinguishable
//     WaitReason DrainingForMaintenance; they are not deleted and not
//     force-placed onto a node that cannot fit them.
//
// Calling it again (or after a restart) is a resume, not a restart: the flag
// and pod phases are durable, only still-waiting pods are retried, and no pod
// is ever migrated twice.
func (c *Controller) StartNodeMaintenance(nodeName string) (*DrainResult, error) {
	res := &DrainResult{Node: nodeName}

	err := c.store.Update(func(st *cluster.State) error {
		n := st.Nodes[nodeName]
		if n == nil {
			return &ErrNodeNotFound{Node: nodeName}
		}

		res.FlagAlreadySet = n.UnderMaintenance
		if !n.UnderMaintenance {
			n.UnderMaintenance = true
			c.logger.Printf("maintenance input: node=%s cordoned for planned maintenance (flag persisted; no new placements)",
				nodeName)
		} else {
			c.logger.Printf("maintenance input: node=%s resume drain (flag was already set; only waiting pods are retried)",
				nodeName)
		}

		// Phase A: detach every Running pod on this node, in deterministic UID
		// order, releasing its reservation and stamping the distinguishable
		// planned-drain wait reason. Placement happens in phase B, together with
		// any drain-origin pods that were already waiting (resume after restart,
		// or capacity appearing elsewhere), so each unbound pod is attempted
		// exactly once per pass — never migrated twice, never double-counted.
		var running []string
		for _, p := range st.Pods {
			if p.NodeName == nodeName && p.Phase == cluster.PhaseRunning {
				running = append(running, p.UID)
			}
		}
		sort.Strings(running)
		detached := map[string]bool{}
		for _, uid := range running {
			p := st.Pods[uid]
			n.Used = n.Used.Sub(p.Requests)
			p.NodeName = ""
			p.OriginNode = nodeName
			p.Phase = cluster.PhaseRescheduling
			p.WaitReason = cluster.WaitReasonDraining
			p.Generation++
			detached[uid] = true
		}

		// Phase B: collect every unbound pod belonging to this drain — both the
		// ones detached above and prior waiters — and place each exactly once.
		var pending []string
		for _, p := range st.Pods {
			if p.Phase == cluster.PhaseRescheduling &&
				p.OriginNode == nodeName &&
				p.WaitReason == cluster.WaitReasonDraining {
				pending = append(pending, p.UID)
			}
		}
		sort.Strings(pending)
		for _, uid := range pending {
			perr := c.scheduler.PlaceInState(st, uid)
			if perr == nil {
				label := "REBOUND"
				if detached[uid] {
					label = "MIGRATED"
					res.Migrated = append(res.Migrated, uid)
				} else {
					res.Rebound = append(res.Rebound, uid)
				}
				c.logger.Printf("maintenance decision: pod=%s %s -> node=%s (deterministic selection)",
					uid, label, st.Pods[uid].NodeName)
				continue
			}
			// "No feasible node" is an expected drain outcome: keep the pod
			// waiting with its distinguishable drain reason.
			if se, ok := perr.(*scheduler.ScheduleError); ok {
				res.Waiting = append(res.Waiting, uid)
				c.logger.Printf("maintenance decision: pod=%s WAITING reason=DrainingForMaintenance origin=%s aggregate=%s (no healthy node fits; retained, not dropped)",
					uid, nodeName, se.AggregateReason())
				continue
			}
			return perr // abort the whole drain on unexpected errors
		}

		// Snapshot what (if anything) is still running on the node after the
		// pass; convergence means this list is empty.
		for _, p := range st.Pods {
			if p.NodeName == nodeName && p.Phase == cluster.PhaseRunning {
				res.StillRunning = append(res.StillRunning, p.UID)
			}
		}
		sort.Strings(res.StillRunning)
		res.UnderMaintenance = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(res.Migrated)
	sort.Strings(res.Rebound)
	sort.Strings(res.Waiting)

	c.logger.Printf("maintenance result: node=%s alreadySet=%t migrated=%d rebound=%d waiting=%d stillRunning=%d",
		nodeName, res.FlagAlreadySet, len(res.Migrated), len(res.Rebound), len(res.Waiting), len(res.StillRunning))
	return res, nil
}

// EndNodeMaintenance brings a node back into service after planned maintenance.
// It ONLY clears the durable maintenance gate: it does not move any running
// pod back onto the node. This is the deliberate anti-flap rule — already
// migrated pods stay where they are, so there is no "move back immediately,
// then move away again" churn. Pods that are still waiting (from this drain or
// from a node loss) become eligible again and are (re)bound by the next
// reconcile using the normal deterministic selection, which may now pick this
// node only when it genuinely wins.
//
// Returns changed=false when the node was not under maintenance. The node also
// needs Ready=true (healthy heartbeats) to receive pods again; ending
// maintenance on a still-unhealthy node only uncordons the gate, and the
// failure detector continues to own readiness as usual.
func (c *Controller) EndNodeMaintenance(nodeName string) (changed bool, err error) {
	err = c.store.Update(func(st *cluster.State) error {
		n := st.Nodes[nodeName]
		if n == nil {
			return &ErrNodeNotFound{Node: nodeName}
		}
		if !n.UnderMaintenance {
			c.logger.Printf("maintenance input: node=%s end-maintenance is a no-op (flag not set)", nodeName)
			return nil
		}
		n.UnderMaintenance = false
		changed = true
		c.logger.Printf("maintenance decision: node=%s uncordoned (back in scheduling pool; running pods NOT moved back — anti-flap; waiting pods re-eligible)",
			nodeName)
		return nil
	})
	return changed, err
}
