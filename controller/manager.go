package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/feng111627/go-cluster-scheduler-go/api"
	"github.com/feng111627/go-cluster-scheduler-go/store"
)

// Manager is the cluster administration surface: register nodes/pods,
// report heartbeats and edit desired replica sets. All mutations are
// persisted through the store so they survive process restarts.
type Manager struct {
	st  *store.Store
	now func() time.Time
}

// NewManager constructs a Manager.
func NewManager(st *store.Store, now func() time.Time) *Manager {
	if now == nil {
		now = time.Now
	}
	return &Manager{st: st, now: now}
}

// AddNode registers a new node. It is an error if the name already exists.
func (m *Manager) AddNode(ctx context.Context, n api.Node) error {
	if n.Name == "" {
		return errors.New("manager: empty node name")
	}
	if n.Capacity.CPU < 0 || n.Capacity.Memory < 0 {
		return errors.New("manager: negative capacity")
	}
	return m.st.Update(func(snap *api.Snapshot) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, ok := snap.Nodes[n.Name]; ok {
			return fmt.Errorf("manager: node %q already exists", n.Name)
		}
		node := n
		node.Used = api.Resources{}
		if node.Phase == "" {
			node.Phase = api.NodeReady
		}
		if node.LastHeartbeat.IsZero() {
			node.LastHeartbeat = m.now()
		}
		snap.Nodes[node.Name] = &node
		return nil
	})
}

// Heartbeat records a node liveness signal at the current time and flips the
// node back to Ready.
func (m *Manager) Heartbeat(ctx context.Context, nodeName string) error {
	return m.HeartbeatAt(ctx, nodeName, m.now())
}

// HeartbeatAt records a heartbeat with an explicit timestamp (demo/test use).
func (m *Manager) HeartbeatAt(ctx context.Context, nodeName string, at time.Time) error {
	return m.st.Update(func(snap *api.Snapshot) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, ok := snap.Nodes[nodeName]
		if !ok {
			return fmt.Errorf("manager: node %q not found", nodeName)
		}
		n.LastHeartbeat = at
		n.Phase = api.NodeReady
		n.NotReadySince = time.Time{}
		return nil
	})
}

// MarkNodeNotReady forcefully marks a node NotReady (used by tests/demo to
// simulate a detected failure without waiting for heartbeat timeout). The
// heartbeat is aged to "a long time ago" so the reconciler treats it as a
// real down node rather than a recovered one; recover it with Heartbeat.
func (m *Manager) MarkNodeNotReady(ctx context.Context, nodeName string) error {
	return m.st.Update(func(snap *api.Snapshot) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, ok := snap.Nodes[nodeName]
		if !ok {
			return fmt.Errorf("manager: node %q not found", nodeName)
		}
		n.Phase = api.NodeNotReady
		if n.NotReadySince.IsZero() {
			n.NotReadySince = m.now()
		}
		n.LastHeartbeat = time.Time{} // aged out: simulates stopped heartbeats
		return nil
	})
}

// RemoveNode deletes a node from the cluster. Pods on it are handled by the
// reconciler (binding to a missing node is treated as lost).
func (m *Manager) RemoveNode(ctx context.Context, nodeName string) error {
	return m.st.Update(func(snap *api.Snapshot) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, ok := snap.Nodes[nodeName]; !ok {
			return fmt.Errorf("manager: node %q not found", nodeName)
		}
		delete(snap.Nodes, nodeName)
		return nil
	})
}

// AddTaint appends t to the node's taints if an identical one is absent.
func (m *Manager) AddTaint(ctx context.Context, nodeName string, t api.Taint) error {
	return m.st.Update(func(snap *api.Snapshot) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, ok := snap.Nodes[nodeName]
		if !ok {
			return fmt.Errorf("manager: node %q not found", nodeName)
		}
		for _, ex := range n.Taints {
			if ex.Key == t.Key && ex.Value == t.Value && ex.Effect == t.Effect {
				return nil
			}
		}
		n.Taints = append(n.Taints, t)
		return nil
	})
}

// RemoveTaint removes matching taints (key + effect) from the node.
func (m *Manager) RemoveTaint(ctx context.Context, nodeName, key string, effect api.TaintEffect) error {
	return m.st.Update(func(snap *api.Snapshot) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, ok := snap.Nodes[nodeName]
		if !ok {
			return fmt.Errorf("manager: node %q not found", nodeName)
		}
		kept := n.Taints[:0]
		for _, t := range n.Taints {
			if t.Key == key && t.Effect == effect {
				continue
			}
			kept = append(kept, t)
		}
		n.Taints = kept
		return nil
	})
}

// ApplyReplicaSet creates or replaces the desired state of a replica set.
func (m *Manager) ApplyReplicaSet(ctx context.Context, rs api.ReplicaSet) error {
	if rs.Name == "" {
		return errors.New("manager: empty replicaset name")
	}
	if rs.Replicas < 0 {
		return errors.New("manager: negative replicas")
	}
	return m.st.Update(func(snap *api.Snapshot) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		cp := rs
		snap.ReplicaSets[rs.Name] = &cp
		return nil
	})
}

// ListPods returns a snapshot copy of all pods sorted by (owner, slot, uid).
func (m *Manager) ListPods() []api.Pod {
	var out []api.Pod
	m.st.View(func(snap *api.Snapshot) {
		for _, p := range snap.Pods {
			out = append(out, *p)
		}
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].OwnerReplica != out[j].OwnerReplica {
			return out[i].OwnerReplica < out[j].OwnerReplica
		}
		if out[i].Slot != out[j].Slot {
			return out[i].Slot < out[j].Slot
		}
		return out[i].UID < out[j].UID
	})
	return out
}

// ListNodes returns a snapshot copy of all nodes sorted by name.
func (m *Manager) ListNodes() []api.Node {
	var out []api.Node
	m.st.View(func(snap *api.Snapshot) {
		for _, n := range snap.Nodes {
			out = append(out, *n)
		}
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
