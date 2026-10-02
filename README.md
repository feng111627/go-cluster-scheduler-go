# go-cluster-scheduler（本地集群 Pod 调度与调谐组件）

一个不依赖任何外部服务的本地集群调度器模拟器：用**内存 + 本地 JSON 文件**模拟节点、Pod
与 Deployment，演示四件事——

1. 把待调度 Pod 确定性地分配到“合适”的节点，拒绝时给出**可区分的原因**；
2. 按期望副本数做**幂等、可并发**的调谐，进程重启后从持久化状态**继续**；
3. 节点失联时在抖动保护下**驱逐并重新调度**，任何时刻实例总数都可解释；
4. 节点**计划内维护（主动摘流/排水）**：持久化维护标记、平滑迁出、放不下时带原因等待，
   恢复后**不立即搬回（防抖动）**，与故障转移互不打架。

## 目录结构

```
internal/cluster/     领域模型 + 单一串行化的持久化状态存储（Store）
  types.go            Node / Pod / Deployment / State / 污点与容忍
  store.go            RWMutex 串行化 + 整状态原子落盘（temp+fsync+rename）
  operations.go       注册节点、Upsert Deployment、心跳、扩缩容、维护开关等 API
internal/scheduler/   过滤、确定性选点、原子绑定、结构化拒绝原因
internal/controller/  期望状态调谐循环 + 节点故障检测/驱逐循环 + 计划内维护/恢复
  maintenance.go      维护排水（drain）与恢复（uncordon）逻辑
cmd/scheduler/        零依赖本地演示（含虚拟时钟；-scenario=maintenance 演示维护生命周期）
```

## 一、调度规则

对每个候选节点按**固定顺序**做过滤，命中即记录该节点的拒绝原因：

| 顺序 | 检查 | 拒绝原因（`scheduler.Reason`） |
|----|------|--------------------------------|
| 1 | 节点 `UnderMaintenance == true`（计划内维护，已摘流） | `NodeUnderMaintenance` |
| 2 | 节点 `Ready == false`（失联/未就绪） | `NodeNotReady` |
| 3 | `Capacity.CPU - Used.CPU < pod.Requests.CPU` | `InsufficientCPU` |
| 4 | `Capacity.Memory - Used.Memory < pod.Requests.Memory` | `InsufficientMemory` |
| 5 | 存在 Pod 未容忍的 `NoSchedule`/`NoExecute` 污点 | `TaintNotTolerated` |

维护门排在最前：维护是运维的**显式、持久**意图，即便维护期间节点心跳变旧也必须继续
拒绝新放置；维护结束后该门解除，其余检查照常生效。

- **污点/容忍语义（类 Kubernetes）**：`key` 必须匹配；容忍项指定了 `effect` 时 effect
  也要匹配；`operator=Exists` 忽略 value，`Equal`（默认）要求 value 相等。
- **没有任何节点通过时**返回 `*scheduler.ScheduleError`，其中 `Failures` 给出
  **每个节点**的 `{node, reason, detail}`；`AggregateReason()` 按固定优先级
  （污点 > CPU > 内存 > 未就绪 > 维护中 > `NoNodes`）汇总成一个可区分的类别。
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

## 四、节点计划内维护与恢复语义

与“等节点自己掉线”不同，计划内维护是运维**主动**把一个健康节点平滑摘出去。核心 API：

- `Controller.StartNodeMaintenance(node)`（进入维护 / 排水 drain）
- `Controller.EndNodeMaintenance(node)`（维护结束 / 解除摘流 uncordon）
- 底层持久化开关：`Store.SetMaintenanceFlag(name, on)`（幂等，返回本次是否发生翻转）

### 进入维护：一次原子、确定性的排水

`StartNodeMaintenance` 在**同一个串行写事务**内完成，因此并发的维护/恢复/调谐不可能
看到撕裂的中间态：

1. **打维护标记** `Node.UnderMaintenance=true`（随全量状态落盘）。调度器的过滤链
   立刻把该节点排除——**不再接收任何新实例**（拒绝原因 `NodeUnderMaintenance`，
   与“节点没心跳”的 `NodeNotReady` 明确区分）。
2. **平滑迁出**：把仍在该节点上的 Running Pod 按 **UID 字典序**逐个摘下
   （先释放该节点已占资源），再走与正常调度**完全相同**的调度器重新选点绑定。
   目标节点因此是**确定性**的：剩余 CPU 最多 → 剩余内存最多 → 节点名最小；
   同一份输入重跑，每个实例落到哪个节点完全一致（`TestMaintenance_DeterministicTargets`）。
3. **放不下就带原因等待**：如果其余健康节点资源放不下，Pod **不删除、不硬塞**，保持
   `Rescheduling`，`OriginNode` 记录迁出节点，`WaitReason=DrainingForMaintenance`。
   后续再次 drain 或调谐会重试（同事务、每实例每轮只尝试一次），容量出现后自动完成；
   等待原因与非计划故障的 `NodeLost` 可区分。

排水结果 `DrainResult` 给出本轮 `migrated`（新迁出并绑定）、`rebound`（历史等待者本轮
绑定，例如重启后续跑或别处腾出容量）、`waiting`（仍放不下）、`stillRunning`
（仍留在维护节点上的实例，收敛后必为 0）。

### 任意时刻账目对得上

维护全过程沿用同一套本地状态，并由不变量约束（单测 `assertInvariants` 每步校验）：

- 非终态实例**总数恒等于期望副本数**，不凭空多/少；
- 每个节点的 `Used` 恒等于其上 Running Pod 请求量之和（没有幽灵占用）；
- 维护节点上**不存在** Running Pod；未绑定的 Pod 不残留 `NodeName`；
- `Rescheduling` 的 Pod 必带可区分的 `WaitReason`，重新绑定后该字段被清空。

### 持久化与“接着走”

维护标记、Pod 相位、`OriginNode`/`WaitReason` 都在同一个 JSON 状态文件里。进程中途
重启后：标记仍然有效（不会“重启就当没维护”），再调 `StartNodeMaintenance` 是
**续跑而非重来**——已迁走的不会二次迁移，只有仍在等待的实例继续重试
（`TestMaintenance_PersistsAcrossRestart`）。

### 维护结束：重新入池，但防抖动

`EndNodeMaintenance` **只清除维护门**，绝不把已迁走的 Running Pod 立刻搬回去——
这就是“刚恢复就搬回、过一会儿又搬走”抖动的防线。具体的再平衡判断稳定可预期：

- **已经在别处 Running 的实例原地不动**；只有本就处于 `Rescheduling` 的等待实例
  （本次排水或节点故障遗留）在后续调谐中重新参与确定性选点，此时恢复节点只有在
  **规则上真正胜出**（资源最空等）时才会被选中；
- 绑定之后再重复调谐是零变化的 no-op，分布一旦收敛就不再抖动
  （`TestMaintenance_RejoinNoImmediateMoveback`）。

注意维护门与就绪门相互独立：解除维护时若节点心跳仍旧，它只是不再被“摘流”，
是否 `Ready` 仍由故障检测器按心跳判定，两条规则各自单向生效、不互相覆盖。

### 与既有故障转移 / 抖动保护的关系和兼容范围

- **两套机制独立、并存、不打架**：维护看 `UnderMaintenance`（运维意图），故障转移看
  心跳与 `Ready`。维护期间故障检测 `DetectNodeFailures` 对该节点**整体让路**：不标记
  不可达、不驱逐、不“自愈”，维护窗口内心拍变旧被忽略，等维护结束再重新按心跳评估
  （`TestMaintenance_DetectorHandsOff`）。
- **等待原因区分来源**：计划内排水 = `DrainingForMaintenance`；超时失联被驱逐 =
  `NodeLost`。二者都复用同一个 `Rescheduling` 相位与同一套确定性重调度。
- **并发安全沿用既有保证**：所有变更都经过 Store 的单一写锁串行化并原子落盘；
  并发 drain / uncordon / reconcile / 扩缩容不会重复迁移、超量创建或超卖
  （`TestMaintenance_ConcurrentCordonReconcileUncordon`，`go test -race` 覆盖）。
- 兼容范围说明：本轮只实现“节点级、无容忍例外”的维护——维护会迁走该节点上的所有
  Running 实例，不区分工作负载；污点/容忍仍按原规则影响新放置。

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
# 4a) 默认场景：确定性调度 / 并发幂等 / 失联故障转移 / 容量不足 / 重启恢复
go run ./cmd/scheduler -state-file cluster-state.json
cat cluster-state.json   # 查看持久化的全量状态

# 4b) 计划内维护场景：排水迁出 -> 放不下带原因等待 -> 中途重启续跑
#     -> 维护结束重新入池（不立即搬回）-> 重复调谐零抖动
go run ./cmd/scheduler -scenario=maintenance
cat cluster-maintenance-state.json
```

演示脚本依次覆盖：

1. 调谐出 4 个副本并确定性放置；
2. 5 个协程**并发重复调谐** → 仍是 4 个，无重复；
3. node-a 心跳静默 15s（超心跳超时、在驱逐宽限内）→ 仅标记不可达，总数仍 4；
4. 宽限内调谐 → 不超量创建；
5. 静默满 30s → 3 个 Pod `Running->Rescheduling` 并重调度，总数恒为 4；
6. 新增 20 副本且容忍污点的 `batch` 负载 → 打满后输出
   `InsufficientCPU` 原因直方图；
7. 用持久化文件**模拟进程重启** → 不重建，计数与重启前完全一致。

## 六、测试场景对照

| 需求场景 | 测试 |
|----------|------|
| CPU/内存不足按原因区分 | `TestSchedule_InsufficientCPU` / `...Memory`、`TestReconcile_InsufficientCapacityLeavesPending` |
| 污点拒绝与容忍放行 | `TestSchedule_TaintRejectionAndToleration`、`TestTolerates` |
| 节点未就绪 | `TestSchedule_NotReadyNode` |
| 计划内维护：标记持久化/拒绝新放置 | `TestSchedule_MaintenanceGateRejects`、`TestStore_MaintenanceFlagPersists` |
| 计划内维护：迁出 + 账目不变量 | `TestMaintenance_DrainsAndMigrates` |
| 迁出目标确定性（同输入同结果） | `TestMaintenance_DeterministicTargets` |
| 放不下带原因等待、容量出现后续跑 | `TestMaintenance_InsufficientCapacityWaits` |
| 维护中重启：标记生效、续跑不重迁 | `TestMaintenance_PersistsAcrossRestart` |
| 恢复重新入池、不立即搬回（防抖） | `TestMaintenance_RejoinNoImmediateMoveback` |
| 维护与故障检测让路、等待原因区分 | `TestMaintenance_DetectorHandsOff` |
| 并发维护/恢复/调谐一致性（-race） | `TestMaintenance_ConcurrentCordonReconcileUncordon` |
| 维护操作命中未知节点 | `TestMaintenance_UnknownNode` |
| 故障驱逐 + 重新调度 + 总数可解释 | `TestNodeFailure_EvictionAndReschedule` |
| 短暂抖动不反复驱逐 | `TestNodeFailure_FlapDoesNotEvict` |
| 并发调谐不重复/不超量 | `TestReconcile_ConcurrentNoDuplicates`（`-race`） |
| 并发调度不超卖 | `TestSchedule_ConcurrentNoOversell`（`-race`） |
| 重启从持久化状态继续 | `TestReconcile_RestartContinuesFromDisk`、`TestStore_PersistAndReload` |
| 事务失败回滚 | `TestStore_FailedUpdateRollsBack` |

`go test -v ./...` 的日志会逐业务打印覆盖情况：调度输入、每个候选节点的 PASS/REJECT
与原因/剩余资源，调谐的创建/绑定/未调度计数，故障检测的标记/宽限/驱逐决策，以及维护
排水的 `MIGRATED/REBOUND/WAITING` 决策、等待原因（`DrainingForMaintenance` vs
`NodeLost`）、恢复时的 uncordon 与防搬回结果；失败时 `DrainResult` 的
`migrated/rebound/waiting/stillRunning` 让迁移进度完全可核对。

所有测试日志通过 `t.Logf` 打印**调度输入**（Pod 请求、容忍、候选节点列表）与
**判定依据**（每个节点 PASS/REJECT、原因、剩余资源、污点、最终决策规则）。
