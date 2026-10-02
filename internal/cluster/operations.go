package cluster

import (
	"fmt"
	"time"
)

// UpsertNode registers or replaces a node definition and stamps a fresh
// heartbeat so the node begins Ready (unless explicitly added NotReady).
func (s *Store) UpsertNode(n *Node) error {
	return s.Update(func(st *State) error {
		if n == nil || n.Name == "" {
			return fmt.Errorf("node requires a name")
		}
		cp := *n
		if cp.LastSeen.IsZero() {
			cp.LastSeen = s.now().UTC()
		}
		if cp.Labels == nil {
			cp.Labels = map[string]string{}
		}
		st.Nodes[cp.Name] = &cp
		return nil
	})
}

// Heartbeat records that a node reported liveness at the store's current time.
func (s *Store) Heartbeat(nodeName string) error {
	return s.HeartbeatAt(nodeName, s.now())
}

// HeartbeatAt records a heartbeat at an explicit time (used by tests to
// simulate clock progression deterministically).
func (s *Store) HeartbeatAt(nodeName string, at time.Time) error {
	return s.Update(func(st *State) error {
		n := st.Nodes[nodeName]
		if n == nil {
			return fmt.Errorf("unknown node: %s", nodeName)
		}
		n.LastSeen = at.UTC()
		return nil
	})
}

// RemoveNode deletes a node definition (used by scenarios/tests only).
func (s *Store) RemoveNode(name string) error {
	return s.Update(func(st *State) error {
		delete(st.Nodes, name)
		return nil
	})
}

// SetMaintenanceFlag durably toggles a node's planned-maintenance flag. It
// returns changed=false when the flag already had the requested value, so
// repeated/concurrent calls are idempotent. The flag alone only stops new
// placements; live-pod migration is performed by the controller's drain.
func (s *Store) SetMaintenanceFlag(name string, on bool) (changed bool, err error) {
	err = s.Update(func(st *State) error {
		n := st.Nodes[name]
		if n == nil {
			return fmt.Errorf("unknown node: %s", name)
		}
		if n.UnderMaintenance == on {
			return nil
		}
		n.UnderMaintenance = on
		changed = true
		return nil
	})
	return changed, err
}

// UpsertDeployment registers or replaces the desired state for a deployment.
func (s *Store) UpsertDeployment(d *Deployment) error {
	return s.Update(func(st *State) error {
		if d == nil || d.Name == "" {
			return fmt.Errorf("deployment requires a name")
		}
		if d.Replicas < 0 {
			return fmt.Errorf("replicas must be >= 0")
		}
		cp := *d
		if cp.Namespace == "" {
			cp.Namespace = "default"
		}
		if cp.Template.NamePrefix == "" {
			cp.Template.NamePrefix = cp.Name
		}
		st.Deployments[cp.Name] = &cp
		return nil
	})
}

// ScaleDeployment changes only the desired replica count.
func (s *Store) ScaleDeployment(name string, replicas int) error {
	return s.Update(func(st *State) error {
		d := st.Deployments[name]
		if d == nil {
			return fmt.Errorf("unknown deployment: %s", name)
		}
		if replicas < 0 {
			return fmt.Errorf("replicas must be >= 0")
		}
		d.Replicas = replicas
		return nil
	})
}

// CountPods returns phase -> count of pods (cluster-wide).
func (s *Store) CountPods() map[PodPhase]int {
	out := map[PodPhase]int{}
	s.View(func(st *State) {
		for _, p := range st.Pods {
			out[p.Phase]++
		}
	})
	return out
}
