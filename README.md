# go-cluster-scheduler

一个**本地单机**的集群 Pod 调度器与调谐器（reconciler）演示实现。节点与 Pod
状态保存在内存中并持久化到一个本地 JSON 文件，不依赖任何外部组件。目标是让
"谁被调度到哪里、为什么被拒绝、故障时实例如何迁移"在任何时刻都**一致且可解释**。

## 目录结构

| 路径 | 职责 |
| --- | --- |
| `api/types.go` | 领域类型：`Node` / `Pod` / `ReplicaSet` / 污点与容忍 / 资源量 / 快照 |
| `scheduler/scheduler.go` | 确定性调度：谓词过滤（就绪、CPU、内存、污点）+ 打分选节点 |
| `store/store.go` | 持久化：读写锁 + 副本提交 + 临时文件 rename 原子写 JSON |
| `controller/manager.go` | 管理面：增删节点、心跳、污点、期望副本（ReplicaSet） |
| `controller/reconciler.go` | 控制循环：调谐、扩缩容、故障转移、防抖、并发安全 |
| `main.go` / `clock.go` | 端到端演示（含虚拟时钟与"进程重启"模拟） |
| `*_test.go` | 单元测试：资源不足、污点、节点故障、并发调谐、重启恢复 |

## 调度规则（scheduler）

对每个候选节点**按名称排序后**依次执行以下谓词，第一个不满足的条件即为该节点
的拒绝原因，所有节点的判定结果都会记录在 `Decision.Results` 中：

1. **节点就绪**：`Node.Phase == Ready`，否则拒绝 `NodeNotReady`。
2. **CPU 充足**：`capacity.cpu - used.cpu >= pod.request.cpu`，否则
   `InsufficientCPU`（日志含 requested/free 数值）。
3. **内存充足**：同理，否则 `InsufficientMemory`。
4. **污点容忍**：节点上每个 `NoSchedule`/`NoExecute` 污点都必须能被 Pod 的某个
   `Toleration` 匹配，否则 `UntoleratedTaint`。
   - 操作符 `Equal`（默认）要求 key/value/effect 全等；`Exists` 只要求 key/effect
     匹配，value 任意；容忍项 effect 留空表示匹配所有 effect。

**选节点（确定性）**：在全部通过的节点中，依次按以下规则比较，保证相同输入永远
得到相同结果，与 map 遍历顺序无关：

1. 剩余 CPU 多者优先；
2. 剩余 CPU 相同则剩余内存多者优先；
3. 仍相同则节点名字典序小者优先。

没有任何节点通过时返回 `*scheduler.ErrNoNodeFit`，其错误串逐节点列出拒绝原因，
例如：

```
scheduler: pod big(...) fits no node: cpu-short=InsufficientCPU(requested=10,free=5) mem-short=InsufficientMemory(...)
```

调度器在每次调用时通过注入的 `Logger` 打印**调度输入**（Pod、请求量、容忍数、
候选数）与**判定依据**（逐节点 free 资源/拒绝原因/最终选择）。

## 调谐语义（reconciler）

### 槽位模型：重复调谐 / 并发调谐不会产生重复实例

每个被 ReplicaSet 管理的 Pod 都带 `(OwnerReplica, Slot)`。期望副本数为 N 时，
槽位固定为 `[0, N)`，**每个槽位至多有一个非终态 Pod**：

- 扩容：在空槽位创建 1 个 Pod；
- 缩容：按槽位号**从大到小**把多余 Pod 置为 `Failed`；
- 发现同一槽位出现多个非终态 Pod（异常兜底）：保留 Running 优先、UID 最小者，
  其余置 `Failed`。

因此同一个调谐动作执行两遍：第二遍发现槽位已被占用，什么都不会再创建。

### 单事务提交

一次 `ReconcileOnce` 分两段：

1. **读锁内规划**：检测节点健康、逐槽位产生 create/bind/evict 动作，并在同一份
   视图里模拟本轮已分配的资源做调度；
2. **单事务提交**：所有动作在一次 `store.Update` 中落库（原子 rename），提交前
   再次校验槽位是否已被占用、节点是否仍就绪；无任何动作时**不写盘、Gen 不增长**。

调谐器另有一把 `runMu` 串行化整轮（规划+提交），store 的读写锁保证它与管理面
的变更互相隔离。Pod UID 由持久化的单调计数器 `NextPodSeq` 生成
（`pod-000001`…），跨重启也不重复。

### 容量可解释

每次提交后，各节点 `Used` 都由"该节点上所有非终态已绑定 Pod 的请求之和"**重算**，
驱逐后立即释放，重复调谐不会重复累加或泄漏容量。

`Report` 输出每轮的事件（创建/调度/驱逐原因）与观测总数
`running / pending / failed` 和每个 ReplicaSet 的活跃实例数 `activePerRS`。

### 故障转移与防抖动

节点健康由心跳与相位共同判定，有两个阈值：

- **HeartbeatTimeout（默认 30s）**：Ready 节点心跳超过该时长未刷新 → 标记
  `NotReady`（记录 `NotReadySince`）；
- **NodeGracePeriod（默认 30s）**：节点**持续** NotReady 超过宽限期才算 `lost`，
  此时其上 Pod 才被驱逐。

故障转移在**同一轮**内完成"驱逐 + 同槽位创建替身 + 调度到健康节点"：

- 宽限期内节点 NotReady：Pod 保持 Running，不驱逐、不重复创建；
- 短暂抖动后心跳恢复：节点回到 Ready、清空计时，绑定关系原封不动；
- 宽限期后失联：旧 Pod 置 `Failed`（`reason=NodeLost`，保留可审计记录），同槽位
  的新 Pod 立即调度到健康节点——驱逐与重建之间，活跃实例总数始终满足
  `activePerRS[rs] == 期望副本数`（被驱逐的瞬间新 Pod 已在同一事务内创建为
  Pending，随后变为 Running）；
- 健康节点都装不下时，替身保持 `Pending` 并带 `Unschedulable` 原因，后续轮次
  持续重试，**不会重复创建**；
- 节点被直接删除：绑定到不存在节点的 Pod 同样按 NodeLost 处理。

另外，节点被加上 Pod 不容忍的 `NoExecute` 污点时，Pod 会被驱逐并重建；匹配
容忍的 Pod 不受影响。`NoExecute` 对新调度同样起到禁入作用，避免"调度上去立刻
被驱逐"的抖动。

### 进程重启

所有节点、Pod、ReplicaSet、`NextPodSeq`、心跳时间都在状态文件里。重启后打开
同一文件即可继续：槽位里已有非终态 Pod，不会从头重建，UID 延续，调谐结果可
直接衔接（见 `TestReconciler_RestartResumesFromDisk`）。

状态文件写入采用"临时文件 + fsync + rename"，崩溃时只可能留下旧文件或新文件，
不会出现写坏的 JSON。

## 本地验证

需要 Go 1.25+。

```bash
# 1. 全部单元测试（-v 可看到调度输入与判定依据的日志）
go test ./...
go test -race ./...          # 并发竞态检测

# 只跑某个场景
go test ./scheduler/ -run TestSchedule_InsufficientCPUAndMemoryAreDistinguishable -v
go test ./controller/ -run TestReconciler_FailoverAfterGrace -v
go test ./controller/ -run TestReconciler_ConcurrentPassesDoNotDuplicate -v

# 2. 端到端演示：初始化 → 扩容 → 重复调谐 → 节点失联（宽限内不驱逐）
#    → 超时故障转移 → 抖动恢复 → 模拟重启恢复
rm -rf .data && go run .

# 3. 不删除 .data 再跑一次，可看到 "resumed from persisted state"
go run .
```

持久化状态位于 `.data/cluster-state.json`（已在 `.gitignore` 中忽略），可以直接
打开检查每个 Pod 的 phase、绑定节点、reason/message 与槽位号。

## 测试场景对照

| 需求 | 测试 |
| --- | --- |
| CPU / 内存不足按原因区分 | `TestSchedule_InsufficientCPUAndMemoryAreDistinguishable` |
| 污点拒绝与容忍放行（Equal/Exists） | `TestSchedule_UntoleratedTaint` |
| 多候选确定性选节点 | `TestSchedule_DeterministicChoice` / `TestSchedule_LeastLoadedWins` |
| 节点故障宽限后重调度、同槽位替身 | `TestReconciler_FailoverAfterGrace` |
| 短暂抖动不驱逐、恢复不迁移 | `TestReconciler_FlutterWithinGraceCausesNoEviction` |
| 实例总数始终可解释 | `TestReconciler_InstanceTotalsAlwaysExplainable` |
| 并发调谐不重复/不超量 | `TestReconciler_ConcurrentPassesDoNotDuplicate`（`-race`） |
| 重启从持久化状态继续 | `TestReconciler_RestartResumesFromDisk`、`TestStore_PersistAcrossReopen` |
| 缩容与 NoExecute 污点 | `TestReconcile_ScaleInRemovesHighestSlots`、`TestReconciler_NoExecuteTaintEvictsAndReschedules` |

## 已知简化

- 单进程内模拟，没有真实网络与 kubelet；心跳由 `Manager.Heartbeat*` 表示。
- Pod 一旦调度成功即为 Running（绑定即运行），没有 Pending/Scheduling 的异步
  窗口，因此并发窗口完全由 store 事务覆盖。
- `Failed/Succeeded` 的历史 Pod 会保留在状态文件中用于审计，目前没有 GC。
