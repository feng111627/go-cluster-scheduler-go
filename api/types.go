// Package api defines the core domain types of the local cluster scheduler:
// nodes, pods, taints/tolerations and resource requests.
package api

import "time"

// Resources is an amount of compute resource measured in whole units
// (CPU: milli-cores, Memory: bytes).
type Resources struct {
	CPU    int64 `json:"cpu"`
	Memory int64 `json:"memory"`
}

// Add returns r + other.
func (r Resources) Add(other Resources) Resources {
	return Resources{CPU: r.CPU + other.CPU, Memory: r.Memory + other.Memory}
}

// Sub returns r - other (may be negative).
func (r Resources) Sub(other Resources) Resources {
	return Resources{CPU: r.CPU - other.CPU, Memory: r.Memory - other.Memory}
}

// TaintEffect enumerates node taint effects.
type TaintEffect string

const (
	// TaintEffectNoSchedule forbids scheduling new pods that do not tolerate the taint.
	TaintEffectNoSchedule TaintEffect = "NoSchedule"
	// TaintEffectNoExecute evicts already running pods that do not tolerate the taint.
	TaintEffectNoExecute TaintEffect = "NoExecute"
)

// Taint marks a node so that only tolerating pods can run on it.
type Taint struct {
	Key    string      `json:"key"`
	Value  string      `json:"value"`
	Effect TaintEffect `json:"effect"`
}

// TolerationOperator enumerates toleration match operators.
type TolerationOperator string

const (
	// TolerationOpEqual requires key/value/effect to match a taint.
	TolerationOpEqual TolerationOperator = "Equal"
	// TolerationOpExists requires only that a taint with the key (and effect) exists.
	TolerationOpExists TolerationOperator = "Exists"
)

// Toleration lets a pod ignore a matching node taint.
type Toleration struct {
	Key      string             `json:"key"`
	Value    string             `json:"value"`
	Effect   TaintEffect        `json:"effect"`
	Operator TolerationOperator `json:"operator"`
}

// NodePhase is the lifecycle phase of a node.
type NodePhase string

const (
	NodeReady    NodePhase = "Ready"
	NodeNotReady NodePhase = "NotReady"
)

// Node is a worker machine in the simulated cluster.
type Node struct {
	Name          string    `json:"name"`
	Capacity      Resources `json:"capacity"`
	Used          Resources `json:"used"`
	Taints        []Taint   `json:"taints,omitempty"`
	Phase         NodePhase `json:"phase"`
	LastHeartbeat time.Time `json:"lastHeartbeat"`
	// NotReadySince is when the node was first observed NotReady; zero when Ready.
	NotReadySince time.Time `json:"notReadySince,omitempty"`
}

// PodPhase is the lifecycle phase of a pod.
type PodPhase string

const (
	PodPending   PodPhase = "Pending"
	PodRunning   PodPhase = "Running"
	PodFailed    PodPhase = "Failed"
	PodSucceeded PodPhase = "Succeeded"
)

// IsTerminal reports whether the phase is a terminal state.
func (p PodPhase) IsTerminal() bool {
	return p == PodFailed || p == PodSucceeded
}

// Pod is a schedulable unit of work.
type Pod struct {
	UID          string       `json:"uid"`
	Name         string       `json:"name"`
	Namespace    string       `json:"namespace"`
	Request      Resources    `json:"request"`
	Tolerations  []Toleration `json:"tolerations,omitempty"`
	NodeName     string       `json:"nodeName,omitempty"`
	Phase        PodPhase     `json:"phase"`
	OwnerReplica string       `json:"ownerReplica,omitempty"` // replica-set name for owned pods
	Slot         int          `json:"slot,omitempty"`         // stable replica slot index
	CreatedAt    time.Time    `json:"createdAt"`
	BoundAt      time.Time    `json:"boundAt,omitempty"`
	// Reason/Message explain the current condition (e.g. why it is Failed
	// or why a Pending pod could not be scheduled).
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// ReplicaSet is the desired state of a group of identical pods.
type ReplicaSet struct {
	Name      string  `json:"name"`
	Namespace string  `json:"namespace"`
	Replicas  int     `json:"replicas"`
	Template  PodSpec `json:"template"`
}

// PodSpec is the immutable template used to create pods.
type PodSpec struct {
	NamePrefix  string       `json:"namePrefix"`
	Request     Resources    `json:"request"`
	Tolerations []Toleration `json:"tolerations,omitempty"`
}

// Snapshot is the whole persistent cluster state written to disk.
type Snapshot struct {
	Nodes       map[string]*Node       `json:"nodes"`
	Pods        map[string]*Pod        `json:"pods"`
	ReplicaSets map[string]*ReplicaSet `json:"replicaSets"`
	Gen         int64                  `json:"gen"`
	// NextPodSeq is a monotonic counter used to mint unique pod UIDs even
	// across repeated reconciles and process restarts.
	NextPodSeq int64 `json:"nextPodSeq"`
}
