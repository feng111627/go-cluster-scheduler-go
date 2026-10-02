// Package scheduler binds pending pods to feasible simulated nodes with a
// deterministic node-selection rule and distinguishable failure reasons.
package scheduler

import (
	"errors"
	"fmt"
	"sort"

	"github.com/feng111627/go-cluster-scheduler-go/internal/cluster"
)

// Reason is a machine-readable scheduling rejection reason.
type Reason string

const (
	// ReasonNodeNotReady: node is not Ready (down / unreachable).
	ReasonNodeNotReady Reason = "NodeNotReady"
	// ReasonNodeMaintenance: node is under planned maintenance (draining).
	ReasonNodeMaintenance Reason = "NodeMaintenance"
	// ReasonInsufficientCPU: node has insufficient allocatable CPU.
	ReasonInsufficientCPU Reason = "InsufficientCPU"
	// ReasonInsufficientMemory: node has insufficient allocatable memory.
	ReasonInsufficientMemory Reason = "InsufficientMemory"
	// ReasonTaintNotTolerated: node carries a taint the pod does not tolerate.
	ReasonTaintNotTolerated Reason = "TaintNotTolerated"
)

// NodeFailure explains why one candidate node was rejected.
type NodeFailure struct {
	Node   string `json:"node"`
	Reason Reason `json:"reason"`
	Detail string `json:"detail"`
}

// ScheduleError reports that no node could take the pod, with a per-node
// reason so callers and logs can distinguish the failure class.
type ScheduleError struct {
	PodUID   string        `json:"podUid"`
	Failures []NodeFailure `json:"failures"`
}

func (e *ScheduleError) Error() string {
	return fmt.Sprintf("scheduling failed for pod %s: all %d candidate node(s) rejected", e.PodUID, len(e.Failures))
}

// AggregateReason returns a deterministic, distinguishable summary of the
// failure class in a fixed precedence:
// TaintNotTolerated > InsufficientCPU > InsufficientMemory > NodeMaintenance >
// NodeNotReady, or "NoNodes" when there are no candidate nodes at all.
// Resource shortages outrank NodeMaintenance so that "healthy nodes cannot
// fit the pod" stays distinguishable from "node is deliberately drained".
func (e *ScheduleError) AggregateReason() Reason {
	has := func(r Reason) bool {
		for _, f := range e.Failures {
			if f.Reason == r {
				return true
			}
		}
		return false
	}
	switch {
	case has(ReasonTaintNotTolerated):
		return ReasonTaintNotTolerated
	case has(ReasonInsufficientCPU):
		return ReasonInsufficientCPU
	case has(ReasonInsufficientMemory):
		return ReasonInsufficientMemory
	case has(ReasonNodeMaintenance):
		return ReasonNodeMaintenance
	case has(ReasonNodeNotReady):
		return ReasonNodeNotReady
	default:
		return Reason("NoNodes")
	}
}

// ErrPodNotSchedulable is returned when the pod exists but is Running/Failed
// and therefore cannot be scheduled right now.
var ErrPodNotSchedulable = errors.New("pod not schedulable")

// Logger is the minimal logging surface used by the scheduler.
type Logger interface {
	Printf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// Scheduler picks nodes and performs the binding inside a single store update.
type Scheduler struct {
	store  *cluster.Store
	logger Logger
}

// New constructs a Scheduler.
func New(st *cluster.Store, logger Logger) *Scheduler {
	if logger == nil {
		logger = nopLogger{}
	}
	return &Scheduler{store: st, logger: logger}
}

// tolerates reports whether the toleration list tolerates taint t.
// Semantics (Kubernetes-like): key must match; effect must match when the
// toleration specifies one; operator Exists ignores value, Equal (default)
// requires equal value.
func tolerates(tols []cluster.Toleration, t cluster.Taint) bool {
	for _, tol := range tols {
		if tol.Key != t.Key {
			continue
		}
		if tol.Effect != "" && tol.Effect != string(t.Effect) {
			continue
		}
		op := tol.Operator
		if op == "" {
			op = "Equal"
		}
		switch op {
		case "Exists":
			return true
		case "Equal":
			if tol.Value == t.Value {
				return true
			}
		}
	}
	return false
}

// feasible checks maintenance mode, node readiness, resources and taints in a
// fixed order and returns the first rejection reason, or "" if the node is
// feasible. The fixed order makes the verdict per node unambiguous and
// explainable.
func feasible(node *cluster.Node, req cluster.Resources, tol []cluster.Toleration) (Reason, string) {
	if node.Maintenance {
		return ReasonNodeMaintenance, "node is under planned maintenance (draining)"
	}
	if !node.Ready {
		return ReasonNodeNotReady, "node is not Ready"
	}
	remaining := node.Capacity.Sub(node.Used)
	if remaining.CPU < req.CPU {
		return ReasonInsufficientCPU, fmt.Sprintf(
			"requested %dm CPU but only %dm of %dm allocatable",
			req.CPU, remaining.CPU, node.Capacity.CPU)
	}
	if remaining.Memory < req.Memory {
		return ReasonInsufficientMemory, fmt.Sprintf(
			"requested %dMi memory but only %dMi of %dMi allocatable",
			req.Memory, remaining.Memory, node.Capacity.Memory)
	}
	// Both NoSchedule and NoExecute taints block new placements: an
	// untolerated NoExecute taint would evict the pod immediately anyway.
	for _, t := range node.Taints {
		if t.Effect != cluster.TaintNoSchedule && t.Effect != cluster.TaintNoExecute {
			continue
		}
		if !tolerates(tol, t) {
			return ReasonTaintNotTolerated, fmt.Sprintf(
				"untolerated taint key=%s value=%s effect=%s",
				t.Key, t.Value, t.Effect)
		}
	}
	return "", ""
}

type candidate struct {
	node      *cluster.Node
	remaining cluster.Resources
}

// ScheduleOne runs PlaceInState inside its own serialized transaction.
func (sc *Scheduler) ScheduleOne(podUID string) error {
	return sc.store.Update(func(st *cluster.State) error {
		return sc.PlaceInState(st, podUID)
	})
}

// PlaceInState runs the whole filter/select/bind sequence against a state that
// is ALREADY inside a store.Update transaction. This lets callers (e.g. the
// controller) compose creation and placement into one atomic, serialized
// transaction, which is what makes concurrent reconciliation safe.
func (sc *Scheduler) PlaceInState(st *cluster.State, podUID string) error {
	pod := st.Pods[podUID]
	if pod == nil {
		return &ScheduleError{PodUID: podUID, Failures: nil}
	}
	if pod.Phase != cluster.PhasePending && pod.Phase != cluster.PhaseRescheduling {
		return fmt.Errorf("%w: pod %s is %s", ErrPodNotSchedulable, podUID, pod.Phase)
	}

	// Deterministic iteration order.
	names := make([]string, 0, len(st.Nodes))
	for n := range st.Nodes {
		names = append(names, n)
	}
	sort.Strings(names)

	sc.logger.Printf("schedule input: pod uid=%s name=%s phase=%s requests={cpu:%dm mem:%dMi} tolerations=%d candidateNodes=%v",
		pod.UID, pod.Name, pod.Phase, pod.Requests.CPU, pod.Requests.Memory, len(pod.Tolerations), names)

	failures := make([]NodeFailure, 0, len(names))
	ok := make([]candidate, 0, len(names))
	for _, name := range names {
		n := st.Nodes[name]
		reason, detail := feasible(n, pod.Requests, pod.Tolerations)
		if reason != "" {
			failures = append(failures, NodeFailure{Node: name, Reason: reason, Detail: detail})
			sc.logger.Printf("schedule verdict: node=%s REJECT reason=%s detail=%s ready=%t used={cpu:%dm mem:%dMi} capacity={cpu:%dm mem:%dMi} taints=%v",
				name, reason, detail, n.Ready, n.Used.CPU, n.Used.Memory, n.Capacity.CPU, n.Capacity.Memory, n.Taints)
			continue
		}
		rem := n.Capacity.Sub(n.Used)
		ok = append(ok, candidate{node: n, remaining: rem})
		sc.logger.Printf("schedule verdict: node=%s PASS remaining={cpu:%dm mem:%dMi}",
			name, rem.CPU, rem.Memory)
	}

	if len(ok) == 0 {
		err := &ScheduleError{PodUID: podUID, Failures: failures}
		sc.logger.Printf("schedule decision: pod=%s UNSCHEDULABLE aggregate=%s failures=%d",
			podUID, err.AggregateReason(), len(failures))
		return err
	}

	// Deterministic tie-break: most remaining CPU, then most remaining
	// memory, then lexicographically smallest node name.
	sort.Slice(ok, func(i, j int) bool {
		a, b := ok[i], ok[j]
		if a.remaining.CPU != b.remaining.CPU {
			return a.remaining.CPU > b.remaining.CPU
		}
		if a.remaining.Memory != b.remaining.Memory {
			return a.remaining.Memory > b.remaining.Memory
		}
		return a.node.Name < b.node.Name
	})
	chosen := ok[0]

	// Bind.
	chosen.node.Used = chosen.node.Used.Add(pod.Requests)
	pod.NodeName = chosen.node.Name
	pod.OriginNode = ""
	pod.Phase = cluster.PhaseRunning
	pod.Generation++

	sc.logger.Printf("schedule decision: pod=%s BOUND node=%s remainingBefore={cpu:%dm mem:%dMi} rule=mostRemainingCPU>mostRemainingMem>name tieBreakCandidates=%d",
		podUID, chosen.node.Name, chosen.remaining.CPU, chosen.remaining.Memory, len(ok))
	return nil
}
