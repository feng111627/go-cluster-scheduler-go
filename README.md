# go-cluster-scheduler（本地集群 Pod 调度与调谐组件）

一个不依赖任何外部服务的本地集群调度器模拟器：用**内存 + 本地 JSON 文件**模拟节点、Pod
与 Deployment，演示三件事——

1. 把待调度 Pod 确定性地分配到“合适”的节点，拒绝时给出**可区分的原因**；
2. 按期望副本数做**幂等、可并发**的调谐，进程重启后从持久化状态**继续**；
3. 节点失联时在抖动保护下**驱逐并重新调度**，任何时刻实例总数都可解释；
4. 节点**计划内维护**（主动下线/重新上线）：平滑迁走实例、标记持久化、恢复不抖动。

## 目录结构

```
internal/cluster/     领域模型 + 单一串行化的持久化状态存储（Store）
  types.go            Node / Pod / Deployment / State / 污点与容忍
  store.go            RWMutex 串行化 + 整状态原子落盘（temp+fsync+rename）
  operations.go       注册节点、Upsert Deployment、心跳、扩缩容等 API
internal/scheduler/   过滤、确定性选点、原子绑定、结构化拒绝原因
internal/controller/  期望状态调谐循环 + 节点故障检测/驱逐循环
  controller.go       调谐 + 失联检测/驱逐
  maintenance.go      计划内维护：EnterMaintenance / ExitMaintenance
cmd/scheduler/        零依赖本地演示（含虚拟时钟）
```

## 一、调度规则

对每个候选节点按**固定顺序**做过滤，命中即记录该节点的拒绝原因：

| 顺序 | 检查 | 拒绝原因（`scheduler.Reason`） |
|----|------|--------------------------------|
| 1 | 节点 `Maintenance == true`（计划内维护中） | `NodeMaintenance` |
| 2 | 节点 `Ready == false`（失联/未就绪） | `NodeNotReady` |
| 3 | `Capacity.CPU - Used.CPU < pod.Requests.CPU` | `InsufficientCPU` |
| 4 | `Capacity.Memory - Used.Memory < pod.Requests.Memory` | `InsufficientMemory` |
| 5 | 存在 Pod 未容忍的 `NoSchedule`/`NoExecute` 污点 | `TaintNotTolerated` |

- **污点/容忍语义（类 Kubernetes）**：`key` 必须匹配；容忍项指定了 `effect` 时 effect
  也要匹配；`operator=Exists` 忽略 value，`Equal`（默认）要求 value 相等。
- **没有任何节点通过时**返回 `*scheduler.ScheduleError`，其中 `Failures` 给出
  **每个节点**的 `{node, reason, detail}`；`AggregateReason()` 按固定优先级
  （污点 > CPU > 内存 > 维护 > 未就绪 > `NoNodes`）汇总成一个可区分的类别。
  资源不足排在维护之前，是为了让“健康节点放不下”始终能与“节点被维护摘除”区分开。
- **选点确定性**：在全部可行节点中，依次按
  **剩余 CPU 最多 → 剩余内存最多 → 节点名字典序最小** 选取。
  相同输入永远得到相同节点，不存在随机/抖动。
- **原子绑定**：过滤 + 容量扣减 + 绑定在 `Store.Update` 的同一个写事务内完成，
  并发调度不可能超卖（`go test -race` 覆盖）。

## 二、调谐（Reconciliation）语义

`Controller.ReconcileDeployment(name)` 在**一个串行事务**内完成三阶段：

1. 统计该 Deployment 的**非终态** Pod（`Pending/Running/Rescheduling`，不含 `Failed`）；
2. 用**稳定序号身份**补齐差额：UID 恒为 `<namespace>-<deployment>-<ordinal>`，
   优先占用最小的空闲 ordinal；
3. 按 UID 排序，把所有 `Pending/Rescheduling` Pod 放入调度器绑定。

由此保证：

- **重复调谐无副作用**：差额始终根据已持久化状态重新计算，身份稳定，不会重建。
- **并发调谐安全**：整个调谐在 Store 的单一写锁内串行化；不会出现重复实例或超量创建。
- **重启继续而非重来**：每次事务提交都把全量状态原子写入 JSON 文件；新进程 `NewStore`
  启动时载入，`epoch`/`podSeq` 继续递增，扩容时复用已有序号，不会重新创建已有副本。
- **容量不足时不丢实例**：放不下的副本保持 `Pending`（故障迁来的保持
  `Rescheduling`），总数仍等于期望数；下一次调谐再尝试，且不会为这些未绑定副本
  重复创建。

## 三、节点故障与故障转移语义

故障检测 `Controller.DetectNodeFailures()` 用两个阈值区分“抖动”和“真故障”：

- `HeartbeatTimeout`（默认 10s）：心跳超过该时长 → 标记 `Ready=false` 并**固定**
  `UnreachableSince`（只在首次越界时改变，防止抖动反复写状态）。
- `EvictionGrace`（默认 30s）：连续不可见时间达到该宽限期才**驱逐一次**
  （`Node.Evicted` 保证幂等，重复检测不会二次驱逐）。

驱逐动作：

- Pod **不删除**，只做 `Running -> Rescheduling`，记录 `OriginNode`，并释放节点已用资源；
- 随后下一次调谐把这些实例重新调度到健康节点（放不下就停留在 `Rescheduling`，不复制）。

抖动保护：节点在宽限期内恢复心跳（`LastSeen` 重新变新）→ 直接恢复 `Ready`，
**不驱逐、不重建**，实例零抖动。

**实例总数在任意时刻都可解释**：驱逐与重建之间 Pod 始终存在，只是相位从 Running
变为 Rescheduling，因此恒有
`Running + Pending + Rescheduling == 期望副本数`（演示与单测会打印该计数）。

持久化采用“写临时文件 → `fsync` → `rename` 覆盖 → 目录 `fsync`”，崩溃只会留下
完整的旧文件或完整的新文件；事务函数返回错误时内存状态也会回滚，内存与磁盘始终一致。

## 四、节点计划内维护（Maintenance）与恢复语义

故障转移处理的是“节点自己掉线”；维护处理的是“人主动把节点摘出去”。两者共用同一
份持久化状态与“只迁移、不丢失”的实例语义，但触发方式与恢复路径不同。

**进入维护 `Controller.EnterMaintenance(node)`**（单个串行事务内完成）：

1. 置 `Node.Maintenance=true` 并固定 `MaintenanceSince`——随全量状态原子落盘，
   **进程重启后维护标记仍然有效**；
2. 该节点上所有 `Running` 实例 `Running -> Rescheduling`（记录 `OriginNode`、释放
   资源），与故障驱逐一样**只迁移、不删除**，实例总数在任何时刻都可解释；
3. 从事务提交起，调度器以 `NodeMaintenance` 拒绝该节点，**新实例不会再落上去**。

随后下一次调谐把这些实例按既有的确定性规则（剩余 CPU 最多 → 剩余内存最多 → 名字
字典序）迁到**健康且放得下**的节点；若健康节点容量不足，实例停留在 `Rescheduling`
等待，其 `ScheduleError` 的逐节点原因能清楚区分“维护节点被拒”（`NodeMaintenance`）
与“健康节点放不下”（`InsufficientCPU/Memory`）——不丢实例，也不硬塞回维护节点。

**幂等与并发**：`EnterMaintenance` 可重复调用（重启后续走进度，已迁走的不会重迁）；
进入/退出/调谐/故障检测全部串行化在 Store 的单一写锁上，并发触发不会重复迁移或
产生不一致（`go test -race` 覆盖）。

**退出维护 `Controller.ExitMaintenance(node)`**（单个串行事务）：

- 清除维护标记，节点重新成为调度候选；
- 同时把 `Ready/LastSeen/Evicted/UnreachableSince` 重置为健康基线——维护期间积累
  的陈旧心跳历史不会让故障检测器在恢复瞬间误判驱逐；
- **防抖动契约**：恢复**绝不**把 Running 实例迁回该节点。只有未来未绑定的实例
  （新副本、后续驱逐）才可能按确定性规则落上来。再平衡的判断因此稳定可预期：
  运行中的实例永远不因恢复而移动，“刚搬回去又搬走”的来回抖动在设计上不存在。

**与既有故障处理的关系与兼容范围**：

- 维护中的节点被故障检测器**整体跳过**（不标记不可达、不驱逐），避免与主动维护
  双重处理；退出维护后故障检测按全新基线重新生效，抖动保护（心跳超时 + 驱逐宽限）
  语义不变；
- 维护迁移复用 `Rescheduling` 相位与调谐放置路径，实例总数恒等式
  `Running + Pending + Rescheduling == 期望副本数` 在维护过程中同样成立；
- 旧版本持久化的状态文件不含维护字段，反序列化后默认为 `false`，等价于无维护，
  完全向后兼容。

## 五、本地验证方法

需要 Go 1.25+，无任何外部依赖（不需要 etcd/K8s/Docker）。

```bash
# 1) 编译
go build ./...

# 2) 全部单元测试（-v 可看到每次调度的输入与逐节点判定依据）
go test ./...
go test -v ./internal/scheduler/
go test -v ./internal/controller/

# 3) 竞态检测（并发调谐 / 并发调度不超卖 / 并发落盘）
go test -race ./...

# 4) 端到端本地演示（虚拟时钟推进，确定性输出）
go run ./cmd/scheduler -state-file cluster-state.json
cat cluster-state.json   # 查看持久化的全量状态
```

演示脚本依次覆盖：

1. 调谐出 4 个副本并确定性放置；
2. 5 个协程**并发重复调谐** → 仍是 4 个，无重复；
3. node-b **进入计划内维护** → web-3 被排干并迁到 node-a，node-b 上实例清零；
4. node-b **退出维护** → 重新可调度，但已迁走的实例**不会搬回来**（防抖动）；
5. node-a 心跳静默 15s（超心跳超时、在驱逐宽限内）→ 仅标记不可达，总数仍 4；
6. 宽限内调谐 → 不超量创建；
7. 静默满 30s → 4 个 Pod `Running->Rescheduling` 并重调度，总数恒为 4；
8. 新增 20 副本且容忍污点的 `batch` 负载 → 打满后输出
   `InsufficientCPU` 原因直方图；
9. 用持久化文件**模拟进程重启** → 不重建，计数与重启前完全一致。

## 六、测试场景对照

| 需求场景 | 测试 |
|----------|------|
| CPU/内存不足按原因区分 | `TestSchedule_InsufficientCPU` / `...Memory`、`TestReconcile_InsufficientCapacityLeavesPending` |
| 污点拒绝与容忍放行 | `TestSchedule_TaintRejectionAndToleration`、`TestTolerates` |
| 节点未就绪 | `TestSchedule_NotReadyNode` |
| 故障驱逐 + 重新调度 + 总数可解释 | `TestNodeFailure_EvictionAndReschedule` |
| 短暂抖动不反复驱逐 | `TestNodeFailure_FlapDoesNotEvict` |
| 并发调谐不重复/不超量 | `TestReconcile_ConcurrentNoDuplicates`（`-race`） |
| 并发调度不超卖 | `TestSchedule_ConcurrentNoOversell`（`-race`） |
| 重启从持久化状态继续 | `TestReconcile_RestartContinuesFromDisk`、`TestStore_PersistAndReload` |
| 事务失败回滚 | `TestStore_FailedUpdateRollsBack` |
| 维护排干并确定性迁移、总数可解释 | `TestMaintenance_DrainMigratesToHealthyNodes` |
| 维护节点拒绝新实例（可区分原因） | `TestSchedule_MaintenanceNodeRejected` |
| 目标容量放不下时等待且原因可区分 | `TestMaintenance_InsufficientCapacityWaitsWithReason` |
| 维护标记与进度跨重启持久化 | `TestMaintenance_FlagPersistedAcrossRestart` |
| 恢复后重新参与调度且防抖动 | `TestMaintenance_ExitRejoinsWithoutFlap` |
| 维护节点不被故障检测双重处理 | `TestMaintenance_SkippedByFailureDetector` |
| 并发维护/恢复/调谐下的一致性 | `TestMaintenance_ConcurrentEnterExitConsistent`（`-race`） |

所有测试日志通过 `t.Logf` 打印**调度输入**（Pod 请求、容忍、候选节点列表）与
**判定依据**（每个节点 PASS/REJECT、原因、剩余资源、污点、最终决策规则）。
