// Package controller runs the desired-state reconciliation loop and the node
// failure detection/eviction loop over the simulated cluster store.
package controller

import (
	"fmt"
	"sort"
	"time"

	"github.com/feng111627/go-cluster-scheduler-go/internal/cluster"
	"github.com/feng111627/go-cluster-scheduler-go/internal/scheduler"
)

// Logger is the minimal logging surface used by the controller.
type Logger interface {
	Printf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// Controller reconciles deployments and detects node failures.
type Controller struct {
	store     *cluster.Store
	scheduler *scheduler.Scheduler
	logger    Logger
	now       func() time.Time
	// heartbeatTimeout: a node whose heartbeat is older than this is marked
	// unreachable (flap protection: only the first crossing mutates state).
	heartbeatTimeout time.Duration
	// evictionGrace: an unreachable node is only evicted after it has stayed
	// unreachable for this long, so brief flaps never cause eviction/rebuild.
	evictionGrace time.Duration
}

// Config holds controller tunables.
type Config struct {
	HeartbeatTimeout time.Duration
	EvictionGrace    time.Duration
}

// Default tunings for the local simulation.
const (
	defaultHeartbeatTimeout = 10 * time.Second
	defaultEvictionGrace    = 30 * time.Second
)

// New constructs a Controller using the wall clock.
func New(st *cluster.Store, sch *scheduler.Scheduler, cfg Config, logger Logger) *Controller {
	return newController(st, sch, cfg, logger, time.Now)
}

// NewWithClock constructs a Controller whose failure-detection clock is
// supplied explicitly (used by the demo and tests for deterministic time).
func NewWithClock(st *cluster.Store, sch *scheduler.Scheduler, cfg Config, logger Logger, now func() time.Time) *Controller {
	return newController(st, sch, cfg, logger, now)
}

func newController(st *cluster.Store, sch *scheduler.Scheduler, cfg Config, logger Logger, now func() time.Time) *Controller {
	if logger == nil {
		logger = nopLogger{}
	}
	if now == nil {
		now = time.Now
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = defaultHeartbeatTimeout
	}
	if cfg.EvictionGrace <= 0 {
		cfg.EvictionGrace = defaultEvictionGrace
	}
	return &Controller{
		store:            st,
		scheduler:        sch,
		logger:           logger,
		now:              now,
		heartbeatTimeout: cfg.HeartbeatTimeout,
		evictionGrace:    cfg.EvictionGrace,
	}
}

func ownerKey(namespace, name string) string { return namespace + "/" + name }

// DeploymentNotFoundError identifies a reconcile for a missing deployment.
type DeploymentNotFoundError struct{ Name string }

func (e *DeploymentNotFoundError) Error() string {
	return fmt.Sprintf("deployment not found: %s", e.Name)
}

// ReconcileResult summarizes one reconcile pass for explainability/tests.
type ReconcileResult struct {
	Desired     int      `json:"desired"`
	Active      int      `json:"active"`      // non-terminal pods counted
	Created     []string `json:"created"`     // UIDs newly created in this pass
	Bound       []string `json:"bound"`       // UIDs newly bound to nodes in this pass
	Unscheduled []string `json:"unscheduled"` // UIDs still without a healthy node
}

// ReconcileDeployment drives one deployment toward its desired replica count
// inside ONE serialized store transaction:
//
//  1. count non-terminal pods belonging to the deployment;
//  2. create the deficit using stable ordinal identities (ns-name-i);
//  3. place every Pending/Rescheduling pod via the scheduler.
//
// All three phases commit together under the store's single write lock.
// Stable identities mean the deficit is always recomputed from durable state,
// so repeated or concurrent calls are idempotent and can never duplicate,
// over-create, or race on placement.
func (c *Controller) ReconcileDeployment(name string) (*ReconcileResult, error) {
	res := &ReconcileResult{}
	var schedErrs []*scheduler.ScheduleError

	err := c.store.Update(func(st *cluster.State) error {
		dep := st.Deployments[name]
		if dep == nil {
			return &DeploymentNotFoundError{Name: name}
		}
		res.Desired = dep.Replicas

		owned := map[string]bool{}
		usedOrdinals := map[int]bool{}
		unbound := make([]string, 0)
		for _, p := range st.Pods {
			if p.Owner != ownerKey(dep.Namespace, dep.Name) {
				continue
			}
			if p.Phase == cluster.PhaseFailed {
				continue // terminal pods do not count
			}
			owned[p.UID] = true
			res.Active++
			if ord, ok := ordinalOf(p.UID); ok {
				usedOrdinals[ord] = true
			}
			if p.Phase == cluster.PhasePending || p.Phase == cluster.PhaseRescheduling {
				unbound = append(unbound, p.UID)
			}
		}

		deficit := dep.Replicas - res.Active
		c.logger.Printf("reconcile input: deployment=%s desired=%d active=%d deficit=%d ownedPods=%d",
			name, dep.Replicas, res.Active, deficit, len(owned))

		// Phase 2: create exactly the deficit, preferring the lowest free
		// ordinals (deterministic and restart-safe).
		if deficit > 0 {
			for ord := 0; len(res.Created) < deficit; ord++ {
				if usedOrdinals[ord] {
					continue
				}
				uid := replicaUID(dep.Namespace, dep.Name, ord)
				podName := fmt.Sprintf("%s-%03d", dep.Template.NamePrefix, ord)
				st.PodSeq++
				pod := &cluster.Pod{
					UID:         uid,
					Name:        podName,
					Namespace:   dep.Namespace,
					Owner:       ownerKey(dep.Namespace, dep.Name),
					Requests:    dep.Template.Requests,
					Tolerations: dep.Template.Tolerations,
					Phase:       cluster.PhasePending,
					CreatedAt:   c.now().UTC(),
					Generation:  1,
				}
				st.Pods[uid] = pod
				usedOrdinals[ord] = true
				unbound = append(unbound, uid)
				res.Created = append(res.Created, uid)
				c.logger.Printf("reconcile decision: deployment=%s CREATE replica uid=%s (ordinal=%d, podSeq=%d)",
					name, uid, ord, st.PodSeq)
			}
		}

		// Phase 3: place every unbound pod in deterministic UID order, still
		// inside this same transaction, so concurrent reconcilers block on
		// the store instead of racing on capacity or on pod phase.
		sort.Strings(unbound)
		for _, uid := range unbound {
			perr := c.scheduler.PlaceInState(st, uid)
			if perr == nil {
				res.Bound = append(res.Bound, uid)
				continue
			}
			if se, ok := perr.(*scheduler.ScheduleError); ok {
				schedErrs = append(schedErrs, se)
				res.Unscheduled = append(res.Unscheduled, uid)
				c.logger.Printf("reconcile decision: deployment=%s pod=%s still unschedulable aggregate=%s",
					name, uid, se.AggregateReason())
				continue
			}
			return perr // abort the whole reconcile on unexpected errors
		}
		return nil // commit creation + successful placements together
	})
	if err != nil {
		return nil, err
	}

	c.logger.Printf("reconcile result: deployment=%s desired=%d active=%d created=%d bound=%d unscheduled=%d",
		name, res.Desired, res.Active, len(res.Created), len(res.Bound), len(res.Unscheduled))
	if len(schedErrs) > 0 {
		return res, &ReconcileError{Deployment: name, ScheduleErrors: schedErrs}
	}
	return res, nil
}

// ReconcileError aggregates per-pod scheduling failures for one deployment.
type ReconcileError struct {
	Deployment     string                     `json:"deployment"`
	ScheduleErrors []*scheduler.ScheduleError `json:"scheduleErrors"`
}

func (e *ReconcileError) Error() string {
	return fmt.Sprintf("reconcile %s: %d pod(s) remain unschedulable", e.Deployment, len(e.ScheduleErrors))
}

// DetectNodeFailures performs one failure-detection pass:
//
//  1. a Ready node whose last heartbeat is older than heartbeatTimeout becomes
//     unreachable (Ready=false, UnreachableSince pinned to the first miss);
//  2. an unreachable node that heartbeats again BEFORE eviction is restored to
//     Ready with no churn — brief flaps never evict anything;
//  3. an unreachable node older than evictionGrace is evicted exactly once:
//     its pods' resources are released and the pods move to Rescheduling.
//
// It returns the UIDs evicted in this pass. The caller (or the next reconcile)
// reschedules them. At every instant the pod count is explainable: eviction
// never deletes pods, it only moves Running -> Rescheduling.
func (c *Controller) DetectNodeFailures() ([]string, error) {
	var evicted []string
	err := c.store.Update(func(st *cluster.State) error {
		now := c.now().UTC()
		names := make([]string, 0, len(st.Nodes))
		for n := range st.Nodes {
			names = append(names, n)
		}
		sort.Strings(names)

		for _, name := range names {
			n := st.Nodes[name]

			// Nodes under planned maintenance are deliberately out of
			// service: their drain is handled by EnterMaintenance, and the
			// failure detector must not double-handle them (no Unreachable
			// flapping, no eviction) while the flag is set.
			if n.Maintenance {
				continue
			}

			// (1) healthy so far: detect a missed heartbeat. If it is stale,
			// fall through — the staleness may already exceed the grace
			// period (e.g. detector itself was down), in which case this same
			// pass should both mark and evict.
			if n.Ready {
				if now.Sub(n.LastSeen) > c.heartbeatTimeout {
					n.Ready = false
					n.UnreachableSince = n.LastSeen.Add(c.heartbeatTimeout)
					c.logger.Printf("node-failure input: node=%s lastSeen=%s staleFor=%s -> marked Unreachable (grace=%s before eviction)",
						name, n.LastSeen.Format(time.RFC3339), now.Sub(n.LastSeen), c.evictionGrace)
				} else {
					continue
				}
			}

			// (2) heartbeat recovered before eviction -> Ready again, no churn.
			if !n.Evicted && now.Sub(n.LastSeen) <= c.heartbeatTimeout {
				c.logger.Printf("node-failure input: node=%s recovered after flap unreachableFor=%s -> Ready (no eviction, no rebuild)",
					name, now.Sub(n.UnreachableSince))
				n.Ready = true
				n.UnreachableSince = time.Time{}
				continue
			}

			// (2b) an already-evicted node whose heartbeat is fresh again is
			// genuinely back: let it rejoin scheduling. Its pods were moved
			// off long ago and are NOT moved back (anti-flap); only future
			// placements land here. This is also the path by which a node
			// that exited maintenance while still silent rejoins once it
			// actually recovers.
			if n.Evicted {
				if now.Sub(n.LastSeen) <= c.heartbeatTimeout {
					c.logger.Printf("node-failure input: node=%s heartbeat fresh again after eviction -> Ready, rejoins scheduling (no pods moved back)",
						name)
					n.Evicted = false
					n.Ready = true
					n.UnreachableSince = time.Time{}
				}
				continue // eviction itself is idempotent; never evict twice
			}

			// (3) unreachable for less than the grace period -> keep waiting.
			unreachableFor := now.Sub(n.UnreachableSince)
			if unreachableFor < c.evictionGrace {
				c.logger.Printf("node-failure input: node=%s still within eviction grace unreachableFor=%s grace=%s -> waiting",
					name, unreachableFor, c.evictionGrace)
				continue
			}

			// Evict exactly once: release resources and move pods to
			// Rescheduling (never delete), so the total stays explainable.
			// WaitReason=NodeFailure keeps this wait distinguishable from a
			// planned-maintenance drain, persisted across restarts.
			n.Evicted = true
			var moved []string
			for _, p := range st.Pods {
				if p.NodeName != name || p.Phase != cluster.PhaseRunning {
					continue
				}
				n.Used = n.Used.Sub(p.Requests)
				p.OriginNode = name
				p.WaitReason = cluster.WaitNodeFailure
				p.NodeName = ""
				p.Phase = cluster.PhaseRescheduling
				p.Generation++
				moved = append(moved, p.UID)
				evicted = append(evicted, p.UID)
			}
			sort.Strings(moved)
			c.logger.Printf("node-failure decision: node=%s EVICTED unreachableFor=%s podsMoved=%v (Running->Rescheduling; resources released; awaiting reschedule)",
				name, unreachableFor, moved)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(evicted)
	return evicted, nil
}
