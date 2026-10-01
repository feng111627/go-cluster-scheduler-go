// Package controller implements the reconcile control loop: desired-vs-actual
// replica tuning, node failover and concurrency-safe persistence.
//
// # Correctness model
//
// Each owned pod carries a stable replica-set name and a slot index. For a
// replica set wanting N replicas there is exactly one set of slots [0,N), and
// a slot holds at most one non-terminal pod. Scale-out creates a pod in an
// empty slot; scale-in fails the pod of the highest slots; failover fails the
// pod on a lost node and immediately creates one replacement in the SAME slot.
// Because the slot identity persists across the replacement, reconciling
// twice (or concurrently) never creates a second instance: the slot already
// has a non-terminal pod and is left alone.
//
// One reconcile pass is computed under a read lock and then committed in a
// single store transaction (atomic JSON write). Repeated passes are
// idempotent; the advisory reconcile lock serialises passes while node/admin
// mutations remain possible in between.
//
// Flap protection: a node whose heartbeat is stale is marked NotReady, but
// pods on it are only evicted once it has stayed NotReady for NodeGracePeriod.
// A fresh heartbeat flips it back Ready and clears the timer, so a short
// flutter causes neither eviction nor duplicate instances.
package controller

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/feng111627/go-cluster-scheduler-go/api"
	"github.com/feng111627/go-cluster-scheduler-go/scheduler"
	"github.com/feng111627/go-cluster-scheduler-go/store"
)

// Event kinds emitted in a Report.
const (
	EvNodeMarkedNotReady = "NodeMarkedNotReady"
	EvNodeBackReady      = "NodeBackReady"
	EvCreate             = "CreatePod"
	EvSchedule           = "SchedulePod"
	EvEvictNodeLost      = "EvictNodeLost"
	EvEvictTaint         = "EvictNoExecuteTaint"
	EvEvictDuplicate     = "EvictDuplicateSlot"
	EvScaleDown          = "ScaleDown"
	EvUnschedulable      = "Unschedulable"
)

// Config holds reconciler tuning knobs.
type Config struct {
	// HeartbeatTimeout: heartbeat older than now-timeout turns a Ready node NotReady.
	HeartbeatTimeout time.Duration
	// NodeGracePeriod: a node must stay NotReady this long before its pods are evicted.
	NodeGracePeriod time.Duration
	// RequeueInterval is the delay between passes in Run.
	RequeueInterval time.Duration
}

// DefaultConfig returns demo-friendly defaults.
func DefaultConfig() Config {
	return Config{
		HeartbeatTimeout: 30 * time.Second,
		NodeGracePeriod:  30 * time.Second,
		RequeueInterval:  1 * time.Second,
	}
}

// Event is one explainable thing that happened (or was refused) in a pass.
type Event struct {
	Kind   string `json:"kind"`
	RS     string `json:"rs,omitempty"`
	Slot   int    `json:"slot,omitempty"`
	PodUID string `json:"podUid,omitempty"`
	Node   string `json:"node,omitempty"`
	Reason string `json:"reason,omitempty"`
	Detail string `json:"detail,omitempty"`
}

// Counts are the observable instance totals after a pass.
type Counts struct {
	Running int `json:"running"`
	Pending int `json:"pending"`
	Failed  int `json:"failed"`
}

// Report explains one reconcile pass.
type Report struct {
	Gen    int64   `json:"gen"`
	Events []Event `json:"events"`
	Counts Counts  `json:"counts"`
	// ActivePerRS maps replica-set name -> number of non-terminal instances.
	ActivePerRS map[string]int `json:"activePerRs"`
}

// Logger is the minimal logging surface used by the controller.
type Logger interface {
	Printf(format string, args ...any)
}

// Reconciler drives cluster state toward desired state.
type Reconciler struct {
	st  *store.Store
	sch *scheduler.Scheduler
	cfg Config
	log Logger
	now func() time.Time

	runMu sync.Mutex // serialises whole reconcile passes (plan + commit)
}

// New constructs a Reconciler.
func New(st *store.Store, sch *scheduler.Scheduler, cfg Config, log Logger, now func() time.Time) *Reconciler {
	if now == nil {
		now = time.Now
	}
	if cfg.HeartbeatTimeout == 0 {
		cfg = DefaultConfig()
	}
	return &Reconciler{st: st, sch: sch, cfg: cfg, log: log, now: now}
}

func (r *Reconciler) logf(format string, args ...any) {
	if r.log != nil {
		r.log.Printf(format, args...)
	}
}

// ---- plan primitives -------------------------------------------------------

type actionKind int

const (
	actCreate actionKind = iota + 1
	actBind
	actEvict
	actNodePhase
)

type action struct {
	kind      actionKind
	rs        string
	slot      int
	pod       *api.Pod // create: new pod; bind/evict: target pod
	podUID    string
	node      string
	phase     api.NodePhase
	since     time.Time
	evictKind string
	reason    string
	detail    string
}

type nodeHealth struct {
	node  *api.Node // nil if node missing from cluster
	ready bool
	lost  bool // eviction threshold crossed
	// planned phase transition
	newPhase api.NodePhase
	newSince time.Time
}

// ReconcileOnce performs one reconcile pass for one replica set, or all
// replica sets when rsName is empty. It always returns a Report describing
// inputs, decisions and resulting totals, even when nothing changed.
func (r *Reconciler) ReconcileOnce(ctx context.Context, rsName string) (*Report, error) {
	r.runMu.Lock()
	defer r.runMu.Unlock()

	now := r.now()
	report := &Report{Events: []Event{}, ActivePerRS: map[string]int{}}

	var actions []action

	// ---- PHASE 1: plan under a read lock ----------------------------------
	var planErr error
	r.st.View(func(snap *api.Snapshot) {
		if err := ctx.Err(); err != nil {
			planErr = err
			return
		}

		// Node health detection with flap protection.
		health := map[string]*nodeHealth{}
		for name, n := range snap.Nodes {
			h := &nodeHealth{node: n, ready: n.Phase == api.NodeReady}
			stale := now.Sub(n.LastHeartbeat) >= r.cfg.HeartbeatTimeout
			switch {
			case n.Phase == api.NodeNotReady:
				if !stale {
					// A fresh heartbeat arrived: node recovered before grace.
					h.ready = true
					h.newPhase = api.NodeReady
				} else {
					since := n.NotReadySince
					if since.IsZero() {
						since = now
					}
					if !since.Add(r.cfg.NodeGracePeriod).After(now) {
						h.lost = true
					}
				}
			case stale: // Ready in storage but heartbeat timed out
				h.ready = false
				h.newPhase = api.NodeNotReady
				h.newSince = now
			}
			health[name] = h
		}
		for _, h := range health {
			if h.newPhase == api.NodeNotReady {
				actions = append(actions, action{kind: actNodePhase, node: h.node.Name, phase: api.NodeNotReady, since: h.newSince})
			} else if h.newPhase == api.NodeReady {
				actions = append(actions, action{kind: actNodePhase, node: h.node.Name, phase: api.NodeReady})
			}
		}

		// Replica sets in deterministic order.
		rsNames := make([]string, 0, len(snap.ReplicaSets))
		for name := range snap.ReplicaSets {
			if rsName == "" || rsName == name {
				rsNames = append(rsNames, name)
			}
		}
		sort.Strings(rsNames)

		// Group owned non-terminal pods by (rs, slot).
		type slotKey struct {
			rs   string
			slot int
		}
		slots := map[slotKey][]*api.Pod{}
		for _, p := range snap.Pods {
			if p.OwnerReplica == "" || p.Phase.IsTerminal() {
				continue
			}
			if rsName != "" && p.OwnerReplica != rsName {
				continue
			}
			if _, ok := snap.ReplicaSets[p.OwnerReplica]; !ok {
				continue
			}
			k := slotKey{p.OwnerReplica, p.Slot}
			slots[k] = append(slots[k], p)
		}

		// pending holds unbound pods that scheduling must consider.
		type pending struct {
			rs   string
			slot int
			pod  *api.Pod // nil if the replacement still has to be created
		}
		var pendings []pending

		for _, name := range rsNames {
			rs := snap.ReplicaSets[name]
			r.logf("reconcile rs=%s desired=%d", name, rs.Replicas)

			// Slots in [0, desired): ensure exactly one active pod each.
			for slot := 0; slot < rs.Replicas; slot++ {
				pods := slots[slotKey{name, slot}]
				keep := chooseSurvivor(pods)
				// Duplicate safety net: every other active pod is failed.
				for _, p := range pods {
					if keep != nil && p.UID == keep.UID {
						continue
					}
					actions = append(actions, action{
						kind: actEvict, pod: p, podUID: p.UID, rs: name, slot: slot,
						evictKind: EvEvictDuplicate, reason: string(scheduler.RejectNodeNotReady),
						detail: fmt.Sprintf("duplicate active pod in slot %d", slot),
					})
				}

				if keep == nil {
					// Empty slot (scale-out or previous pod evicted): create one pending pod.
					pod := newPodFromTemplate(rs, slot)
					actions = append(actions, action{kind: actCreate, pod: pod, rs: name, slot: slot})
					pendings = append(pendings, pending{rs: name, slot: slot, pod: pod})
					continue
				}

				if keep.NodeName == "" {
					// Unbound pending pod created by an earlier pass.
					pendings = append(pendings, pending{rs: name, slot: slot, pod: keep})
					continue
				}

				// Bound pod: decide whether its node is still acceptable.
				h, known := health[keep.NodeName]
				switch {
				case !known || h.lost:
					node := keep.NodeName
					actions = append(actions, action{
						kind: actEvict, pod: keep, podUID: keep.UID, rs: name, slot: slot,
						evictKind: EvEvictNodeLost, reason: "NodeLost",
						detail: fmt.Sprintf("node %s unreachable beyond grace", node),
					})
					pod := newPodFromTemplate(rs, slot)
					actions = append(actions, action{kind: actCreate, pod: pod, rs: name, slot: slot})
					pendings = append(pendings, pending{rs: name, slot: slot, pod: pod})
				case h.ready && hasUntoleratedNoExecute(h.node.Taints, rs.Template.Tolerations):
					actions = append(actions, action{
						kind: actEvict, pod: keep, podUID: keep.UID, rs: name, slot: slot,
						evictKind: EvEvictTaint, reason: "NoExecute",
						detail: fmt.Sprintf("node %s added untolerated NoExecute taint", keep.NodeName),
					})
					pod := newPodFromTemplate(rs, slot)
					actions = append(actions, action{kind: actCreate, pod: pod, rs: name, slot: slot})
					pendings = append(pendings, pending{rs: name, slot: slot, pod: pod})
				default:
					// Healthy, or NotReady still inside the grace window: leave it.
					r.logf("  slot=%d pod=%s bound node=%s kept", slot, keep.UID, keep.NodeName)
				}
			}

			// Slots >= desired: scale in, highest slots first.
			excess := make([]int, 0)
			for k := range slots {
				if k.rs == name && k.slot >= rs.Replicas {
					excess = append(excess, k.slot)
				}
			}
			sort.Sort(sort.Reverse(sort.IntSlice(excess)))
			for _, slot := range excess {
				for _, p := range slots[slotKey{name, slot}] {
					actions = append(actions, action{
						kind: actEvict, pod: p, podUID: p.UID, rs: name, slot: slot,
						evictKind: EvScaleDown, reason: "ScaleDown",
						detail: fmt.Sprintf("desired=%d slot=%d removed", rs.Replicas, slot),
					})
				}
			}
		}

		// Pending scheduling order: rs name asc, slot asc.
		sort.Slice(pendings, func(i, j int) bool {
			if pendings[i].rs != pendings[j].rs {
				return pendings[i].rs < pendings[j].rs
			}
			return pendings[i].slot < pendings[j].slot
		})

		// Effective node usage = stored used + requests bound earlier in this pass.
		deltaUsed := map[string]api.Resources{}
		candidateOf := func() []*api.Node {
			out := make([]*api.Node, 0, len(health))
			names := make([]string, 0, len(health))
			for name := range health {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				h := health[name]
				if !h.ready {
					continue
				}
				cp := *h.node
				cp.Used = h.node.Used.Add(deltaUsed[name])
				out = append(out, &cp)
			}
			return out
		}

		for _, pe := range pendings {
			dec, err := r.sch.Schedule(ctx, pe.pod, candidateOf())
			if err != nil || dec.Chosen == "" {
				actions = append(actions, action{
					kind: actBind, rs: pe.rs, slot: pe.slot, pod: pe.pod,
					evictKind: EvUnschedulable, reason: "Unschedulable",
					detail: summarizeRejects(dec),
				})
				continue
			}
			deltaUsed[dec.Chosen] = deltaUsed[dec.Chosen].Add(pe.pod.Request)
			actions = append(actions, action{
				kind: actBind, rs: pe.rs, slot: pe.slot, pod: pe.pod, node: dec.Chosen,
			})
		}
	})
	if planErr != nil {
		return nil, planErr
	}

	// Fast path for a fully converged state: a no-op pass must not touch
	// disk or advance the generation, so repeated/background reconciles
	// leave a clean, replayable history.
	if len(actions) == 0 {
		r.st.View(func(snap *api.Snapshot) {
			fillTotals(snap, report)
		})
		return report, nil
	}

	// ---- PHASE 2: commit everything in one transaction --------------------
	commitErr := r.st.Update(func(snap *api.Snapshot) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, a := range actions {
			switch a.kind {
			case actNodePhase:
				n, ok := snap.Nodes[a.node]
				if !ok {
					continue
				}
				n.Phase = a.phase
				if a.phase == api.NodeNotReady {
					n.NotReadySince = a.since
				} else {
					n.NotReadySince = time.Time{}
				}
				ev := Event{Kind: EvNodeMarkedNotReady, Node: a.node}
				if a.phase == api.NodeReady {
					ev.Kind = EvNodeBackReady
				}
				report.Events = append(report.Events, ev)
				r.logf("node %s -> %s", a.node, a.phase)

			case actCreate:
				// A concurrent/admin change could have filled the slot: recheck.
				if slotHasActive(snap, a.rs, a.slot) {
					r.logf("skip create rs=%s slot=%d: slot already filled", a.rs, a.slot)
					continue
				}
				snap.NextPodSeq++
				p := *a.pod
				p.UID = mintPodUID(snap.NextPodSeq)
				p.CreatedAt = now
				// Publish the minted identity on the shared plan object so a
				// later bind action in the SAME pass targets this pod. This is
				// what makes create+schedule atomic in one reconcile.
				a.pod.UID = p.UID
				snap.Pods[p.UID] = &p
				report.Events = append(report.Events, Event{
					Kind: EvCreate, RS: a.rs, Slot: a.slot, PodUID: p.UID,
				})
				r.logf("create pod uid=%s rs=%s slot=%d cpu=%d mem=%d",
					p.UID, a.rs, a.slot, p.Request.CPU, p.Request.Memory)

			case actBind:
				if a.node == "" {
					// Unschedulable. For a created-but-not-yet-stored pod there is
					// nothing to record; existing pending pods simply stay pending.
					if a.pod != nil && a.pod.UID != "" {
						if existing, ok := snap.Pods[a.pod.UID]; ok && !existing.Phase.IsTerminal() {
							existing.Reason = "Unschedulable"
							existing.Message = a.detail
						}
					}
					report.Events = append(report.Events, Event{
						Kind: EvUnschedulable, RS: a.rs, Slot: a.slot,
						PodUID: a.podUID, Reason: "Unschedulable", Detail: a.detail,
					})
					r.logf("rs=%s slot=%d unschedulable: %s", a.rs, a.slot, a.detail)
					continue
				}
				// Bind may target a pod created earlier in this same transaction.
				target := a.podUID
				if target == "" && a.pod != nil {
					target = a.pod.UID
				}
				p, ok := snap.Pods[target]
				if !ok || p.Phase.IsTerminal() || p.NodeName != "" {
					continue
				}
				n, ok := snap.Nodes[a.node]
				if !ok || n.Phase != api.NodeReady {
					// Stale plan (node vanished/flip-flopped): stay pending, retry next pass.
					r.logf("skip bind pod=%s node=%s: node gone/not ready at commit", target, a.node)
					continue
				}
				p.NodeName = a.node
				p.Phase = api.PodRunning
				p.BoundAt = now
				p.Reason = ""
				p.Message = ""
				report.Events = append(report.Events, Event{
					Kind: EvSchedule, RS: a.rs, Slot: a.slot, PodUID: p.UID, Node: a.node,
				})
				r.logf("bind pod=%s -> node=%s", p.UID, a.node)

			case actEvict:
				p, ok := snap.Pods[a.podUID]
				if !ok || p.Phase.IsTerminal() {
					continue
				}
				p.Phase = api.PodFailed
				p.NodeName = ""
				p.Reason = a.reason
				p.Message = a.detail
				report.Events = append(report.Events, Event{
					Kind: a.evictKind, RS: a.rs, Slot: a.slot, PodUID: p.UID,
					Reason: a.reason, Detail: a.detail,
				})
				r.logf("evict pod=%s kind=%s reason=%s detail=%s", p.UID, a.evictKind, a.reason, a.detail)
			}
		}

		// Authoritative capacity accounting: node usage is exactly the sum of
		// requests of non-terminal bound pods. Idempotent under repeated and
		// concurrent passes, and never leaks capacity after eviction.
		recomputeUsage(snap)

		// Final observable totals.
		fillTotals(snap, report)
		report.Gen = snap.Gen
		return nil
	})
	if commitErr != nil {
		return nil, commitErr
	}
	return report, nil
}

// Run loops ReconcileOnce until ctx is cancelled.
func (r *Reconciler) Run(ctx context.Context) error {
	if r.cfg.RequeueInterval <= 0 {
		r.cfg.RequeueInterval = DefaultConfig().RequeueInterval
	}
	ticker := time.NewTicker(r.cfg.RequeueInterval)
	defer ticker.Stop()
	for {
		if _, err := r.ReconcileOnce(ctx, ""); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			r.logf("reconcile error: %v", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// ---- helpers ---------------------------------------------------------------

// chooseSurvivor keeps Running over Pending, then the smallest UID so that a
// duplicate slot collapses deterministically.
func chooseSurvivor(pods []*api.Pod) *api.Pod {
	var keep *api.Pod
	for _, p := range pods {
		if keep == nil {
			keep = p
			continue
		}
		if podRank(p) > podRank(keep) || (podRank(p) == podRank(keep) && p.UID < keep.UID) {
			keep = p
		}
	}
	return keep
}

func podRank(p *api.Pod) int {
	if p.Phase == api.PodRunning {
		return 1
	}
	return 0
}

func newPodFromTemplate(rs *api.ReplicaSet, slot int) *api.Pod {
	return &api.Pod{
		// UID assigned at commit from the monotonic sequence.
		Name:         fmt.Sprintf("%s-%d", rs.Template.NamePrefix, slot),
		Namespace:    rs.Namespace,
		Phase:        api.PodPending,
		Request:      rs.Template.Request,
		Tolerations:  append([]api.Toleration(nil), rs.Template.Tolerations...),
		OwnerReplica: rs.Name,
		Slot:         slot,
	}
}

func mintPodUID(seq int64) string {
	return fmt.Sprintf("pod-%06d", seq)
}

func slotHasActive(snap *api.Snapshot, rs string, slot int) bool {
	for _, p := range snap.Pods {
		if p.OwnerReplica == rs && p.Slot == slot && !p.Phase.IsTerminal() {
			return true
		}
	}
	return false
}

func hasUntoleratedNoExecute(taints []api.Taint, tols []api.Toleration) bool {
	for _, t := range taints {
		if t.Effect != api.TaintEffectNoExecute {
			continue
		}
		if !scheduler.Tolerates(t, tols) {
			return true
		}
	}
	return false
}

func summarizeRejects(d *scheduler.Decision) string {
	if d == nil {
		return "no scheduler decision"
	}
	s := ""
	for _, r := range d.Results {
		if r.Pass {
			continue
		}
		if s != "" {
			s += "; "
		}
		s += r.Node + "=" + string(r.Reason)
		if r.Detail != "" {
			s += "(" + r.Detail + ")"
		}
	}
	if s == "" {
		s = "no candidate nodes"
	}
	return s
}

// fillTotals populates the observable instance totals in rep from snap.
func fillTotals(snap *api.Snapshot, rep *Report) {
	uids := make([]string, 0, len(snap.Pods))
	for uid := range snap.Pods {
		uids = append(uids, uid)
	}
	sort.Strings(uids)
	for _, uid := range uids {
		p := snap.Pods[uid]
		switch {
		case p.Phase == api.PodRunning:
			rep.Counts.Running++
		case !p.Phase.IsTerminal():
			rep.Counts.Pending++
		default:
			rep.Counts.Failed++
		}
		if !p.Phase.IsTerminal() && p.OwnerReplica != "" {
			rep.ActivePerRS[p.OwnerReplica]++
		}
	}
	rep.Gen = snap.Gen
}

// recomputeUsage rebuilds every node's Used resources from non-terminal bound
// pods. Pods bound to a missing node are ignored (the same pass evicts them).
func recomputeUsage(snap *api.Snapshot) {
	for _, n := range snap.Nodes {
		n.Used = api.Resources{}
	}
	uids := make([]string, 0, len(snap.Pods))
	for uid := range snap.Pods {
		uids = append(uids, uid)
	}
	sort.Strings(uids)
	for _, uid := range uids {
		p := snap.Pods[uid]
		if p.Phase.IsTerminal() || p.NodeName == "" {
			continue
		}
		if n, ok := snap.Nodes[p.NodeName]; ok {
			n.Used = n.Used.Add(p.Request)
		}
	}
}
