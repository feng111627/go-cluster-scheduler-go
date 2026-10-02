// Package cluster defines the in-memory/local-file simulated cluster model:
// nodes, pods, deployments and the durable world-state of the scheduler.
package cluster

import "time"

// Quantity is a milli-unit resource quantity, e.g. 1000 == 1 core / 1 byte unit.
type Quantity int64

// Resources is an additive resource vector.
type Resources struct {
	CPU    Quantity `json:"cpu"`    // milli-CPU
	Memory Quantity `json:"memory"` // MiB
}

// Add returns r+x.
func (r Resources) Add(x Resources) Resources {
	return Resources{CPU: r.CPU + x.CPU, Memory: r.Memory + x.Memory}
}

// Sub returns r-x. Callers must ensure the result is meaningful.
func (r Resources) Sub(x Resources) Resources {
	return Resources{CPU: r.CPU - x.CPU, Memory: r.Memory - x.Memory}
}

// TaintEffect determines what a taint does to pods that do not tolerate it.
type TaintEffect string

const (
	// TaintNoSchedule means pods without a matching toleration cannot be scheduled.
	TaintNoSchedule TaintEffect = "NoSchedule"
	// TaintNoExecute means pods without a matching toleration are evicted too.
	TaintNoExecute TaintEffect = "NoExecute"
)

// Taint marks a node; pods must tolerate it (per effect) to stay schedulable.
type Taint struct {
	Key    string      `json:"key"`
	Value  string      `json:"value"`
	Effect TaintEffect `json:"effect"`
}

// Toleration matches a taint by key/value with an operator.
type Toleration struct {
	Key      string `json:"key"`
	Operator string `json:"operator"` // "Equal" (default) or "Exists"
	Value    string `json:"value"`
	Effect   string `json:"effect"` // empty matches any effect
}

// PodPhase is the lifecycle phase of a pod instance.
type PodPhase string

const (
	// PhasePending exists but is not bound to a healthy node yet.
	PhasePending PodPhase = "Pending"
	// PhaseRunning is bound to a node currently considered healthy.
	PhaseRunning PodPhase = "Running"
	// PhaseRescheduling was evicted from a lost node or drained from a node
	// under planned maintenance and awaits a new binding.
	PhaseRescheduling PodPhase = "Rescheduling"
	// PhaseFailed is a terminally failed pod.
	PhaseFailed PodPhase = "Failed"
)

// WaitReason distinguishes WHY an unbound (Rescheduling) pod is waiting, so a
// planned-maintenance drain is never confused with an unplanned node loss.
type WaitReason string

const (
	// WaitReasonNodeLost: pod was evicted because its node stayed unreachable
	// beyond the eviction grace period (unplanned failure).
	WaitReasonNodeLost WaitReason = "NodeLost"
	// WaitReasonDraining: pod was moved off a node that an operator put under
	// planned maintenance, and no other healthy node currently fits it.
	WaitReasonDraining WaitReason = "DrainingForMaintenance"
)

// Node is a simulated worker machine.
type Node struct {
	Name     string            `json:"name"`
	Capacity Resources         `json:"capacity"`
	Used     Resources         `json:"used"`
	Labels   map[string]string `json:"labels,omitempty"`
	Taints   []Taint           `json:"taints,omitempty"`
	Ready    bool              `json:"ready"`
	LastSeen time.Time         `json:"lastSeen"`
	// UnreachableSince is set when the node first misses a heartbeat.
	UnreachableSince time.Time `json:"unreachableSince,omitempty"`
	// Evicted means pods were already moved off this (long-lost) node; it
	// makes eviction idempotent across repeated detection passes.
	Evicted bool `json:"evicted,omitempty"`
	// UnderMaintenance is the operator-issued, durable "planned drain" flag.
	// While set, the node receives no new placements and Running pods are
	// migrated off it. It is independent of Ready/heartbeats (so it survives a
	// restart and does not fight the failure detector) and is cleared only by
	// an explicit maintenance-end call.
	UnderMaintenance bool `json:"underMaintenance,omitempty"`
}

// Pod is a single simulated pod instance.
type Pod struct {
	UID       string `json:"uid"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	// Owner is the "namespace/deploymentName" this replica belongs to.
	Owner       string       `json:"owner,omitempty"`
	Requests    Resources    `json:"requests"`
	Tolerations []Toleration `json:"tolerations,omitempty"`
	Phase       PodPhase     `json:"phase"`
	NodeName    string       `json:"nodeName,omitempty"`
	// OriginNode records the node a Rescheduling pod was evicted/drained from.
	OriginNode string    `json:"originNode,omitempty"`
	CreatedAt  time.Time `json:"createdAt"`
	// WaitReason records why a Rescheduling pod is waiting (node loss vs.
	// planned maintenance drain); cleared on binding.
	WaitReason WaitReason `json:"waitReason,omitempty"`
	// Generation bumps on every state transition; helps make decisions explainable.
	Generation int `json:"generation"`
}

// Deployment is the desired state: a homogeneous replica set.
type Deployment struct {
	Name      string      `json:"name"`
	Namespace string      `json:"namespace"`
	Replicas  int         `json:"replicas"`
	Template  PodTemplate `json:"template"`
}

// PodTemplate is the cookie-cutter for pods of a deployment.
type PodTemplate struct {
	NamePrefix  string       `json:"namePrefix"`
	Requests    Resources    `json:"requests"`
	Tolerations []Toleration `json:"tolerations,omitempty"`
}

// State is the whole durable world. It is persisted atomically as one JSON blob.
type State struct {
	Nodes       map[string]*Node       `json:"nodes"`
	Pods        map[string]*Pod        `json:"pods"`
	Deployments map[string]*Deployment `json:"deployments"`
	// Counters for stable, restart-surviving identity generation.
	PodSeq    int64     `json:"podSeq"`
	Epoch     int64     `json:"epoch"`
	UpdatedAt time.Time `json:"updatedAt"`
}
