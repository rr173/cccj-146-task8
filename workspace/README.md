# FeatForge — 机器学习特征物化与回填服务

特征定义声明**源事件、关联键、窗口、变换版本**。在线写入持续更新当前值；离线回填严格按
**样本时点（point-in-time, PIT）**重建历史值，绝不看到当时尚未发生的数据。相同定义的
在线与离线结果按**确定性抽样规则**自动比对。定义升级创建**新一代际**与旧代并行计算，
覆盖率/偏差门槛达标后才能切换读取别名；回填失败可从**分区检查点**继续，迟到事件只重算
**受影响范围**。读者始终看到一个完整代际，并可随时回滚到先前可验证版本。

上游可对已接收记录提交**事件订正**（`replace`/`retract`，按记录单调递增 `revision`）：
每次操作形成**不可变台账条目**，受理时原子改写事件日志；落地由**可恢复作业**分批刷新
受影响的实体与时间段（在线当前值、在线历史、离线值、所有仍在计算的代际），每批保存
进度，进程崩溃后重启自动续跑。读者永远只看到**完整订正版本**——同号重放返回原结果，
同号不同内容返回 409，已撤销记录仍可审计但不再参与计算。

读取方还能做**特征时间旅行与差异解释**：按实体、样本时刻和指定代际读取一个
**可复现快照**（`snapshot_id` 由快照内容寻址，重复查询永远同号），响应给出窗口边界、
纳入计算的事件、被排除的未来/窗外/已订正事件及其**来源版本**（`record_rev`）。两个
代际在同一时点的**差异查询**在同一只读事务内构建一对完整快照并按键对齐，分页顺序
稳定、游标钉住快照内容哈希与签游标时的服务别名——翻页期间别名切换、订正/新事件
改变事件版本时返回 409 并拒绝旧游标，而不是把新旧代事件清单拼在一起；非法时刻、
不存在或不完整的代际、同代自比、非法/被篡改游标返回明确客户端错误。

## 特征快照搬运（独立模块 `featforge.snapshot`）

把某个特征版本在**给定截止时刻**的物化结果导出为**不可变 bundle**，由**另一个
实例**分块拉取、校验、原子启用。模块零第三方依赖（SQLite + 标准库 HTTP），既可
作为库嵌入，也能以两个独立服务进程跨实例运行；`FeatForgeOfflineSource` 直接
适配现有 FeatForge 的离线 PIT 物化结果。

```
 导出实例                                    导入实例
 ┌────────────────────────┐                ┌──────────────────────────┐
 │ POST /exports           │  manifest(GET) │ POST /imports             │
 │ 幂等键台账(fence)        │ ─────────────▶ │ 持久检查点 + 块租约(fence) │
 │ 物化→分块→内容寻址       │  块字节(GET)   │ acquire → pull → put       │
 │ manifest.json 发布       │ ◀───────────── │ 逐块哈希校验               │
 └────────────────────────┘                │ 全部到齐→三闸门→原子激活   │
                                           │ active 整行切换，读者隔离   │
                                           └──────────────────────────┘
```

- **导出端**
  - manifest 是内容寻址的“合同”：`manifest_hash` 由（特征/版本/截止时刻/参数/
    **固定块序**/每块 sha256/schema 标识）规范化哈希得到；同导出语义永远同 bundle id。
  - `schema_id` 由列定义（名+类型，顺序敏感）决定，导入端不兼容即拒绝。
  - **幂等**：同幂等键 + 同参数返回原 bundle（200 `replayed`）；同键不同参数
    **409 idempotency_conflict**；同键正在生成且 fence 未过期 **409 in_progress**。
  - 生成持 **fencing token**，发布是持当前 fence 的 CAS；进程崩溃/fence 过期后可被
    接管；生成中断只留未引用块，**绝不发布半成品**。
- **导入端**
  - 每块状态（pending/leased/committed）+ 单调 `fence_seq` 持久化在 SQLite；
    执行器无状态、可多副本，崩溃重启从检查点续传，committed 块不重传。
  - **同一块只有一个有效持有者**：领取在 `BEGIN IMMEDIATE` 事务内完成；租约 TTL
    过期后任意执行器可接管（fence +1）；旧持有者的**迟到提交一律 409
    fence_conflict**（fence 同时校验序号与有效期，块已提交也不放过旧 token）。
  - 块提交先验 fence 再验 sha256（坏块 **422 chunk_corrupt**，不污染暂存区）。
  - 激活前过三道闸门：manifest 自哈希/固定块序/块总数/行数合计、逐块重算字节
    哈希与大小、schema 标识；全过才在一个事务里把 `ss_active` 整行替换。
    缺块/损坏/不兼容 → transfer=failed，**active 指针不动，旧 bundle 继续服务**。
  - 读者只引用 active 一行：切换是整行原子替换，切换前后读到各自完整 bundle；
    旧 bundle 物化目录保留，可按 `bundle_id` 继续读取。
- **运行**
  ```bash
  python -m featforge.snapshot exporter --root /data/ex --port 8101 --seed
  python -m featforge.snapshot importer --root /data/im --port 8102
  python -m featforge.snapshot worker --exporter http://localhost:8101 \
      --importer http://localhost:8102 --transfer imp_xxx --executor-id w1
  python scripts/snapshot_demo.py        # 8 个可复现场景，真实两进程
  python -m unittest tests.test_snapshot_transport -v   # 21 个测试
  ```
  演示跑通：正常搬运、重复导出、幂等冲突、生成中断、崩溃续传、租约接管与迟到
  提交、坏块拒绝、激活前后读者隔离（执行器支持 `--hold-seconds/--corrupt/
  --die-after-acquire` 故障注入）。

  | 端 | 方法 & 路径 | 说明 |
  | --- | --- | --- |
  | 导出 | `POST /exports` | 创建导出（`idempotency_key` 幂等；同键异参 409；中断重试） |
  | 导出 | `GET /exports/{key}` | 幂等键状态（building/published） |
  | 导出 | `GET /bundles` · `GET /bundles/{id}` | bundle 列表 / manifest |
  | 导出 | `GET /bundles/{id}/chunks/{i}` | 固定块序中的第 i 块字节（`x-chunk-sha256`） |
  | 导入 | `POST /imports` | 创建搬运会话（从导出端拉 manifest，同 bundle 幂等） |
  | 导入 | `GET /imports` · `GET /imports/{tid}` | 会话列表 / 持久检查点状态 |
  | 导入 | `POST /imports/{tid}/chunks/acquire` | 领取/接管块租约，返回单调 fencing token |
  | 导入 | `PUT /imports/{tid}/chunks/{i}` | 提交块（`x-fence-token`；迟到 409，坏块 422） |
  | 导入 | `POST /imports/{tid}/activate` | 三闸门整体校验 + 原子启用（失败旧 bundle 继续服务） |
  | 导入 | `GET /active` · `/active/{feature}` · `/active/{feature}/rows` | active 总览 / manifest / 读行（可带 `bundle_id` 读旧 bundle） |

## 多租户仲裁器（模块 `featforge.arbiter`）

回填、订正、重算、**快照**四类后台分片作业进入**统一 ready set**（`arb_jobs`），
由每个租户独立的 **权重 / 并行席位 / 虚拟运行时间（vruntime）** 仲裁。选择器
在单个 `BEGIN IMMEDIATE` 事务内先回收心跳超时的分片，再在「有排队、未暂停、
席位未满」的租户中取 `vruntime − 老化速率×等待秒数` 最小者：高权重租户每次
claim 只推进 `1/weight` 的虚拟时间因而获得更多处理量；长期排队的小租户靠
老化积累最终必然被选中（防饥饿）。沉睡租户唤醒时对齐全局 min vruntime，
不会一次性霸占仲裁器。

- **策略暂停**：`pause` 只阻止新的 claim，已 claimed 的分片允许心跳、提交
  进度并写完；`resume` 后排队作业立即可领。在线 `PUT .../policy` 修改
  weight/seats/paused **只改租户行**：同一个 job 绝不会因此被领取第二次，
  `shard_cursor` 与已提交进度原样保留。
- **claim_epoch fencing**：每次 claim（含租约接管）让作业的 epoch 单调 +1；
  心跳/进度/完成都要带 epoch + worker id。心跳超时（`lease_expires_at`）后
  分片回到 queued，旧持有者的任何上报（包括迟到的完成）一律 **409
  stale_epoch**；worker 不符 409 worker_mismatch；非 claimed 状态上报 409
  not_claimed；游标后退 409 cursor_regress（相等游标幂等）。
- **检查点续跑**：分片游标 `shard_cursor` 只能前进，每次推进在
  `arb_progress` 追加台账行；`fail` 保留游标，`retry` 用**同一作业行**重新
  排队，接管方从游标继续，已提交分片绝不重做。进程退出（SIGKILL 语义）只是
  不再心跳，租约超时后被别的执行者接替。
- **入队去重**：`(租户, business_key)` 唯一；同键同负载（规范化 JSON，键序
  无关）重放原作业（全生命周期幂等，完成后重放仍返回原行），同键不同 kind/
  参数/分片数返回 **409 business_key_conflict**；键空间按租户隔离。
- **仲裁历史**：每次 selected/idle/lease_expired/completed/failed 写一行
  `arb_history`（含当时的 vruntime、等待老化、欠额、席位占用与原因），
  自动截断保留；状态页展示各租户的**等待时长、欠额、占用席位与近期选择结果**。
- **既有作业接入**：`featforge.arbiter_bridge.LegacyBridge` 是 opt-in 适配
  层，可把 `jobs` 表中的回填/订正/重算按特征名归属到租户并镜像进统一
  ready set，经仲裁器 claim 后仍由 Store 原有执行器运行（检查点/订正进度/
  自动切换逻辑不变），状态与 epoch 双向同步，租约超时可接管。默认执行路径
  （`Store.claim_next_job`）不受影响。

```bash
python scripts/arbiter_demo.py     # 8 个零依赖可复现场景（真实 SQLite）
python -m pytest tests/test_arbiter.py tests/test_arbiter_worker.py \
                 tests/test_arbiter_api.py tests/test_arbiter_bridge.py -v
```

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /api/v1/arbiter/tenants` | 创建租户（weight/seats，已存在幂等不覆盖） |
| `GET  /api/v1/arbiter/tenants` · `/tenants/{name}` | 租户列表 / 单租户仲裁视图（等待时长/欠额/席位） |
| `PUT  /api/v1/arbiter/tenants/{name}/policy` | 在线改 weight/seats/paused（不动作业与进度） |
| `POST /api/v1/arbiter/tenants/{name}/pause` · `/resume` | 策略暂停（只阻新 claim）/ 恢复 |
| `POST /api/v1/arbiter/jobs` | 入队（tenant/kind/business_key/payload/shards_total；同键重放 200，异参 409） |
| `GET  /api/v1/arbiter/jobs` · `/jobs/{id}` · `/jobs/{id}/progress` | ready set 查询 / 作业详情 / 进度台账 |
| `POST /api/v1/arbiter/claims` | 领取下一项（欠额+老化），返回单调 claim_epoch 与租约到期时间 |
| `POST /api/v1/arbiter/jobs/{id}/heartbeat` | 领取续期（可附带前向游标） |
| `POST /api/v1/arbiter/jobs/{id}/progress` | 进度提交（epoch fence；游标只能前进，相等幂等） |
| `POST /api/v1/arbiter/jobs/{id}/complete` · `/fail` · `/retry` | 完成释放席位 / 显式失败保留游标 / 失败重试 |
| `POST /api/v1/arbiter/reap-expired` | 立即回收心跳超时分片（claim 时也会自动回收） |
| `GET  /api/v1/arbiter/history` · `/status` | 仲裁历史（近期选择结果）/ 状态页数据 |

场景脚本验证：权重分流（3:1 配置下 claim≈3:1，vruntime 收敛）、洪流防饥饿、
并行席位上限与在线调席不重复领取、暂停后已开工分片可写完与恢复、进程退出后
租约超时接替（三任 worker 进度台账齐全）、旧 epoch 心跳/进度/完成全拒绝、
fail→retry 检查点继续、业务键重复入队（重放/409/租户隔离）。

## 容量凭证与预算结算（模块 `featforge.budget`）

每个客户一份预算策略：**按时间补充的计算额度**（`refill_amount` /
`refill_interval`）、**余额突发上限** `burst_cap`、执行席位 `max_seats`。
工单提交时必须声明预计消耗 `estimate`，系统在单个 `BEGIN IMMEDIATE` 事务内
**原子预留**并开出容量凭证（`budget_vouchers`）；所有金额变动写只追加账本
`budget_ledger`（补充/预留/追加预留/结算/退款/突发裁剪），余额永远等于账本
`avail_delta` 重放值，任意一行之后都不允许为负——多提交者并发抢最后一份
额度时由 SQLite 写锁串行化，恰好一份成功，其余进延后区。

- **延后区与自动唤醒**：余额不足的工单状态 `deferred`，标出
  `earliest_ready_at`（按补充锚点逐期模拟，估算与惰性补充共用同一套规则）；
  `estimate > burst_cap` 的工单标 `over_burst_cap`，永不可行。补充采用惰性
  计算（任何操作/后台 tick 先把 `last_refill_at` 按周期整倍推进，最多补一期，
  更早各期受突发上限蒸发），到账后同一事务内按 FIFO pump 延后/暂停凭证，
  够额即自动转 `reserved`，`wait_runnable` 被条件变量及时唤醒。
- **单调进度结算**：`consumed_total` 是累计实际消耗，只能单调前进，相等/回退
  上报幂等不扣费。未超预留直接结算；超预估时原子**追加预留**（余额不足则只
  结算到已预留位置，凭证转 `paused`、`pending_total` 记下执行者想要的位置、
  **立即释放执行席位**，补充到账后自动恢复 `reserved`，必须重新占用）。
- **失联接替**：占用占席位并让 `claim_epoch` 单调 +1；心跳超时（或
  `reap_expired`）后凭证保留在 `executing` 但清空持有者、释放席位，接替者
  重新占用 epoch +1。旧持有者的迟到上报在接替前 409 lease_expired、接替后
  409 stale_epoch（worker 不符 409 worker_mismatch），**已结算金额沿用、
  绝不二次扣费**。
- **终态唯一**：`completed` / `terminated` 互斥且只发生一次最终结算/退款。
  终止只退**尚未使用**的预留（`reserved − settled`），退款让余额回补但受
  `burst_cap` 裁剪（裁掉部分记 `lost_total` 与零金额 cap_clamp 留痕）；重复
  终止幂等返回（`replayed=true, refunded=0`），完成与终止并发竞争时事务决出
  一个胜者，输家拿到终态幂等结果或 409 terminal。
- **在线调预算**：`PUT .../policy` 只改客户行：缩小突发上限会把当前余额向下
  裁剪并留痕，调大上限/补充额会在同事务内尝试解锁延后工单；**绝不撤销或修改
  已发出凭证的预留/结算**。

```bash
python scripts/budget_demo.py     # 7 个零依赖可复现场景（真实 SQLite + 手工时钟）
python -m pytest tests/test_budget.py tests/test_budget_api.py -q
```

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /api/v1/budget/customers` | 配置客户预算（补充额/周期/突发上限/席位/期初余额；已存在幂等不覆盖） |
| `GET  /api/v1/budget/customers` · `/customers/{name}` | 客户列表 / 视图（可用额、已预留未结算、已结算、下次补充、被阻塞工单） |
| `PUT  /api/v1/budget/customers/{name}/policy` | 在线调预算（只影响后续预留，不撤销已发凭证） |
| `POST /api/v1/budget/customers/{name}/vouchers` | 提交工单并原子预留（201 reserved / 202 deferred，同键重放/异参 409） |
| `GET  /api/v1/budget/customers/{name}/vouchers` · `/api/v1/budget/vouchers/{id}` | 凭证列表（可按 state 过滤）/ 凭证详情 |
| `POST .../vouchers/{id}/acquire` · `/heartbeat` | 占用凭证占席位（返回 epoch）/ 心跳续期（失联接替也走 acquire） |
| `POST .../vouchers/{id}/consumed` | 按单调累计消耗结算（settled / paused+确认位置 / idempotent） |
| `POST .../vouchers/{id}/complete` · `/terminate` | 完成最终结算退未用预留（终态唯一）/ 终止退款（幂等，只退未用部分） |
| `POST /api/v1/budget/tick` | 立即执行一轮补充 + pump + 失联回收（后台线程也会周期执行） |
| `GET  /api/v1/budget/ledger` · `/status` | 只追加账本查询（可按客户/凭证过滤）/ 状态页数据 |

场景脚本验证：正常预留结算与退款、余额不足到点自动唤醒（t+59 仍延后、t+60
取得资格）、12 份工单并发抢余额 10 恰好 1 份成功且账本无负余额行、实际消耗超
预估追加成功→再超额暂停释放席位→补充后恢复、进程失联接替沿用已结算且旧 epoch
上报 fence 拒绝、终止只退未用预留且重复终止幂等、完成与终止并发终态与退款只
发生一次。状态页（`GET /`）展示每个客户的**可用额、已预留、已结算、下次补充
时间及被预算阻塞的工单**。

## 架构

```
                    ┌──────────────────────────────────────────────┐
 定义 API ──────────▶│ features / generations / aliases / jobs      │
 (FastAPI)           │ audits / validations / checkpoints           │
                    │                                              │
 事件写入 ───────────▶│ events（事实日志）+ watermark                │
 /api/v1/events      │      │                                       │
                    │      ├─▶ online_current / online_history      │ 在线：当前值
 订正写入 ───────────▶│ corrections（订正台账）+ correction_scopes   │   +刻度快照
 /api/v1/corrections │      │        （所有非退役代并行更新）         │
                    │      ▼                                       │
                    │   jobs 队列 ──claim──▶ Worker（可多副本）       │
                    │      │ backfill: 按水位截断 + 分区检查点        │ 离线：PIT
                    │      │ recompute: 迟到事件只刷受影响刻度        │   历史值
                    │      │ correction: 订正分批落地 + 批次进度      │
                    │      ▼                                       │
                    │   offline_values (gen, entity, asof_ts)       │
                    │                                              │
 读取别名 ◀──────────│ aliases → serving 代（原子整行替换）           │
 online / offline    │ shadow 旧代保留可回滚                          │
 └──────────────────────────────────────────────────────────────────┘
```

- **代际生命周期**：`building`（回填中）→ `candidate`（回填完成待校验）→ `serving`
  （别名指向）；被替下的旧代变为 `shadow`（仍完整，可回滚）。
- **PIT 语义**：时点 `t` 的值只使用 `event_ts <= t` 的事件；baseline 回填在作业开跑时
  按当时 watermark 截断区间，作业本身不会看到未来数据。
- **原子别名**：读者只读 `aliases.serving_gen_id` 指向的代。切换是单行 upsert，
  读者要么读到旧完整代、要么读到新完整代，不会看到半成品。

## 快速开始

```bash
pip install -r requirements.txt

# 端到端演示（定义→写入→回填→升级→门槛拦截→回滚→失败续跑→迟到重算→订正台账）
python scripts/demo.py

# 运行测试
python -m pytest

# 启动一体化服务（API + 执行器）
python -m featforge all
# → 状态页 http://localhost:8000/   交互式 API 文档 http://localhost:8000/docs
```

容器化部署（API 与执行器分离，执行器可多副本）：

```bash
docker compose up --build
```

同一镜像以角色启动：`python -m featforge api | worker | all`。SQLite 数据放在
`/data` 卷；多 worker 副本靠原子 `claim` 去重。

## API 概览

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /api/v1/features` | 创建特征定义（源、关联键、值字段、窗口、变换版本） |
| `POST /api/v1/features/{name}/generations` | 定义升级：创建并行计算的新一代 |
| `POST /api/v1/events` | 写入源事件（幂等 `dedup_key`，自动识别迟到事件） |
| `POST /api/v1/corrections` | 提交事件订正：`replace`/`retract` + 单调 `revision`；同号重放幂等，同号不同内容 409 |
| `GET  /api/v1/corrections?source=&record_key=` | 订正台账 / 按记录查看完整修订链 |
| `GET  /api/v1/corrections/{id}` | 订正详情：受影响区间、批次进度、落地作业 |
| `GET  /api/v1/records/{source}/{key}` | 记录视图：全部内容版本（含已撤销）+ 修订链 |
| `GET  /api/v1/features/{name}/online/{id}` | 在线读取当前值（走 serving 代别名） |
| `GET  /api/v1/features/{name}/offline/{id}?asof_ts=` | 离线 PIT 历史查询 |
| `GET  /api/v1/features/{name}/snapshots/{id}?asof_ts=&gen_no=&limit=&cursor=` | **特征时间旅行**：可复现快照（窗口边界/纳入事件/排除原因/来源版本/`snapshot_id`） |
| `GET  /api/v1/features/{name}/snapshots/{id}/diff?asof_ts=&a_gen_no=&b_gen_no=` | **跨代差异解释**：一对完整快照对齐分页，游标漂移返回 409 |
| `POST /api/v1/generations/{id}/validate` | 覆盖率/偏差 + 在线离线抽样校验 |
| `POST /api/v1/generations/{id}/promote?force=` | 达标后切换别名（可强制）；并发切换输掉竞争返回 409，不写成功历史 |
| `POST /api/v1/features/{name}/rollback` | 回滚到先前可验证版本；并发回滚输掉竞争返回 409 |
| `POST /api/v1/features/{name}/quality-check` | 切换后复检，发现回退自动回滚 |
| `GET  /api/v1/jobs` · `POST /api/v1/jobs/{id}/resume` | 作业状态页数据 / 检查点续跑 |
| `GET  /` | HTML 状态页（代际、作业、水位、校验、时间旅行快照/差异、审计） |

### 15 秒上手

```bash
curl -s localhost:8000/api/v1/features -H 'content-type: application/json' -d '{
  "name":"gmv_1h","source":"purchase","join_key":"user_id",
  "value_field":"amount","window":"1h","transform":"sum","transform_version":1,
  "backfill_start":0,"partition_seconds":3600,"sample_tick_seconds":300}'

curl -s localhost:8000/api/v1/events -H 'content-type: application/json' -d \
  '{"source":"purchase","entity_id":"u1","event_ts":600,"raw":{"amount":42},"dedup_key":"ord-1"}'

curl -s -XPOST localhost:8000/api/v1/worker/tick       # 执行回填→校验→自动切换
curl -s localhost:8000/api/v1/features/gmv_1h/online/u1
curl -s "localhost:8000/api/v1/features/gmv_1h/offline/u1?asof_ts=600"

# 订正已接收记录 ord-1：金额 42 -> 100（revision=1），落地作业分批刷新受影响区间
curl -s -XPOST localhost:8000/api/v1/corrections -H 'content-type: application/json' -d \
  '{"source":"purchase","record_key":"ord-1","revision":1,"op":"replace","raw":{"amount":100}}'
curl -s -XPOST localhost:8000/api/v1/worker/tick       # 执行订正落地
curl -s "localhost:8000/api/v1/corrections?record_key=ord-1"   # 修订链

# 时间旅行：按实体+样本时刻读取第 1 代可复现快照（未来事件被列出并标记 future）
curl -s "localhost:8000/api/v1/features/gmv_1h/snapshots/u1?asof_ts=600&gen_no=1"
# 跨代差异：第 1 代 vs 当前 serving 代，稳定分页（首页游标钉住一对快照）
curl -s "localhost:8000/api/v1/features/gmv_1h/snapshots/u1/diff?asof_ts=600&a_gen_no=1"
```

## 关键设计

### 1. 点内时间（PIT）回填
- 时间为整型 Unix 秒；样本按固定刻度（默认 5 分钟）物化，`asof_ts=t` 只取
  `event_ts <= t` 且落在滚动窗口 `(t-window, t]` 的事件。
- baseline 作业区间末端开放，**开跑时**按当时 watermark 向下对齐到刻度；之后到达的
  事件不会改变已有结果，杜绝训练/推理时的数据穿越。

### 2. 在线/离线同口径自动比对
- 在线侧除当前值外，还在每个事件生效的样本刻度写一份 PIT 快照（`online_history`）。
- 校验时在线快照与离线值按相同 `(实体, 刻度)` 对齐，用 SHA-256 **确定性抽样**
  （同种子、同输入得到同样的对子，结果可复现），统计覆盖率与相对偏差/类别不一致率。

### 3. 代际升级门槛
- 新代回填完成后自动做两件事：新 vs 旧离线值比较（覆盖率 ≥ `min_coverage`、
  数值 p95 偏差或类别不一致率 ≤ `max_bias`），以及在线 vs 离线抽样比对。
- 任一不达标，代际停在 `candidate`，别名不动；可用 `force=true` 人工强切并留审计。
- 切换后 `quality-check` 复检当前 serving 代；若发现质量回退（如在线被污染），
  **自动回滚**到上一完整代。

### 4. 检查点续跑 & 迟到事件
- 回填按分区推进，每个分区完成后写 `checkpoints`；作业失败后 `resume` 只跑缺失分区。
- 迟到事件（`event_ts` 低于 watermark）只对 serving/shadow 代入队一个
  `recompute` 作业，区间为 `[ceil(event_ts, tick), ceil(event_ts+window, tick)]`
  且不超过回填末端；同实体同代重复迟到事件合并区间、去重，不触碰无关分区。
- 落在尚未回填分区的迟到事件不入重算作业，由续跑回填自然覆盖。

### 5. 并发与一致性
- 作业领取是单条 `UPDATE ... SELECT queued LIMIT 1` 的 `BEGIN IMMEDIATE` 事务，
  多个 worker 副本不会重复执行同一作业；事件写入靠 `dedup_key` 幂等。
- 别名切换、回滚都在一个事务内完成“新代 serving + 旧代 shadow + 历史记录”。
- **切换/回滚的竞争条件是操作发起时的服务指针**：请求开始时先解析当前
  serving 代际 id（首次切换记为“无”），写事务（`BEGIN IMMEDIATE`）内重新
  读取并做 CAS 比较。两个已校验版本并发上线时事务被串行化，先提交的一方
  成功；后执行的一方发现指针已被改写，收到 **409 alias_conflict**（消息点明
  发起时指针与当前指针），事务回滚，**不更新别名/代际状态、不写成功历史、
  不记 promoted 审计**——两个调用方绝不会同时拿到成功。输家重新读取当前
  版本后再发起即为顺序切换。同一代重复 promote / 回滚到当前 serving 代均
  **幂等**（`already_serving`，不重复写历史）；任何时刻别名只指向一个完整
  代际，在线读流量始终读到单一、完整、可验证的版本。

### 6. 事件订正台账
- **不可变台账**：每次 `replace`/`retract` 在 `corrections` 表落一条审计条目
  （revision、操作、生效后内容、内容哈希），内容字段写入后不再修改，仅执行状态推进。
- **单调序号**：按 `(source, record_key)` 严格递增。同号同内容重放返回原结果
  （幂等）；同号不同内容或序号回退返回 **409**；并发提交由 `BEGIN IMMEDIATE`
  串行化，序号只单调前进。
- **原子版本**：受理事务内原子改写事件日志（replace 旧行标记 `superseded_by`
  并插入新版本行；retract 标记 `retracted_by`）。所有计算只读有效事件；
  被取代/撤销的行永久保留供审计（`GET /api/v1/records/...`），不再参与计算。
- **可恢复落地**：`correction` 作业按 (代际, 实体, 刻度区间) 分批，每批一个事务
  刷新 `online_current`/`online_history`/`offline_values` 并写 `correction_progress`；
  进程崩溃后 `running` 作业被自动重新排队，已提交批次按进度跳过；失败作业可
  `resume` 续跑。批次在事务内从**当前**事件日志计算，较新订正到达后旧作业的
  剩余批次自然按新日志计算，重叠区间由新订正自己的作业重写——旧任务不会覆盖新结果。
- **读者只见完整版本**：落地完成前，被订正覆盖的 (实体, 刻度) 在读取时直接按
  事件日志实时计算（读穿），任何时刻读到的都是某个完整订正版本，绝不新旧混杂。
- **只刷新受影响范围**：受理时按记录旧/新版本的事件时间算出
  `[ceil(ts), ceil(ts+window)]` 刻度区间，逐 (代际, 实体) 记入
  `correction_scopes`；所有非退役代际（serving/shadow/candidate/building）
  都在范围内，无关实体与分区不动。

### 7. 特征时间旅行与差异解释
- **可复现快照**：`GET .../snapshots/{entity}?asof_ts=&gen_no=` 在**单个只读
  事务**（WAL 一致性读视图）内把别名解析一次、重放该实体的全部有效事件。响应含
  `snapshot_id`（`snap_<代际>_<实体>_<时刻>_<内容哈希前16位>`）、滚动窗口边界
  `(t-window, t]`、快照值、**纳入事件**与**被排除事件**：`future`（晚于样本时刻，
  PIT 绝不穿越）、`out_of_window`（已滑出左开窗口）、`superseded`/`retracted`
  （已被订正取代/撤销）。每条事件带 `record_key`+`record_rev` 标明**来源版本**；
  头部 `source_versions` 汇总实际生效的版本集合。
- **内容寻址、重复同号**：`snapshot_id` 与完整内容哈希由（代际、实体、时刻、窗口、
  值、规范化后的事件清单）决定；相同参数重复查询永远返回同一标识，事件日志改写
  （新事件/订正落地）后标识必然变化。
- **跨代差异（代际变更报告）**：`.../diff?a_gen_no=&b_gen_no=` 在**同一只读事务**
  内为两代各建一个完整快照，再按 `(event_ts, seq, event_id)` 全外连接逐行对齐，
  给出两代值、相对偏差、是否不同及差异统计（值不同/相同条数、合计）；事件口径
  （纳入/排除原因）两代一致，差异只可能来自变换版本。行内 `a_value/b_value` 是
  **截至该行、窗口内已纳入事件的前缀累积特征值**：差异在第一个分叉的纳入事件处
  出现，最后一条纳入事件行的值恰好等于头部 `a/b` 快照值，汇总计数直接数自行
  内容——头部 `a_value/b_value` 已不同（如 sum 版 vs mean 版）时绝不会再把记录
  归入 `value_equal`。被排除事件（future/out_of_window/superseded/retracted）
  两行值为 `null`、`different=false`，不计入相同/不同统计。
- **稳定分页 + 单请求单视图**：分页用不透明自包含游标（base64，内含查询类型
  `kind`、代际 id、**签游标时的 serving 别名 id**、时刻、内容哈希、offset）。
  排序固定、无重复无遗漏；首页与每一页的统计、行内容、`snapshot_id` 完全一致。
  续页请求只认游标，不得再带任何代际参数：期间**服务别名切换**（serving 别名 id
  与签游标时不一致）或**事件版本变化**（订正/新事件改写事件日志导致内容哈希漂移）
  一律返回 **409 SnapshotConflict** 并拒绝续读旧游标，调用方重新发起首页查询
  即可——系统绝不把切换前/切换后的固定视图或新旧事件清单拼进同一个请求。不带
  代际时每个请求在自己的只读事务里解析一次 serving 别名，同一请求只可能使用一个
  完整代际。
- **明确错误**：非整数/早于回填起点/晚于当前时间的时刻、不存在的代际、代际不属于
  该特征、代际回填未完成、同代自比、续页携带代际参数均返回 400；非法游标（不可
  解析、非本端点签发、字段缺失、offset 非整数或越界、上下文被篡改）返回 400
  （消息说明原因）；别名切换/游标视图漂移返回 409。
- **状态页**：`GET /` 的“特征时间旅行 & 代际差异解释”区块直接展示每个 serving
  特征的示例快照标识、事件明细（带来源版本）、排除原因徽标与跨代差异统计。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `FEATFORGE_DB` | `/tmp/featforge.db` | SQLite 库文件 |
| `FEATFORGE_PARTITION_SECONDS` | `3600` | 回填分区长度/检查点粒度 |
| `FEATFORGE_SAMPLE_TICK_SECONDS` | `300` | 样本刻度 |
| `FEATFORGE_MIN_COVERAGE` | `0.95` | 切换覆盖率门槛 |
| `FEATFORGE_MAX_BIAS` | `0.05` | 切换 p95 偏差/类别不一致门槛 |
| `FEATFORGE_SAMPLE_FRACTION` | `0.2` | 在线/离线抽样比例（确定性哈希） |
| `FEATFORGE_WORKER_POLL` | `1.0` | 执行器轮询间隔秒 |
| `FEATFORGE_CORRECTION_BATCH_TICKS` | `12` | 订正落地每批刻度数（批提交即存进度） |
| `FEATFORGE_ARBITER_LEASE_TTL` | `30` | 仲裁器租约 TTL 秒：无心跳的分片超时后可被接管 |
| `FEATFORGE_ARBITER_AGING_RATE` | `0.1` | 欠额+老化选择器的老化速率（等待秒数 × 该值加分） |
| `FEATFORGE_ARBITER_HISTORY_LIMIT` | `500` | 仲裁历史保留条数 |
| `FEATFORGE_ARBITER_SNAPSHOT_DIR` | `/tmp/featforge-snapshots` | 仲裁器快照作业默认产物目录 |
| `FEATFORGE_BUDGET_LEASE_TTL` | `30` | 容量凭证执行租约 TTL 秒：无心跳的占用超时后可被接替（旧 epoch fence） |
| `FEATFORGE_BUDGET_TICK` | `0.2` | 预算后台补充/唤醒线程轮询间隔秒（金额本身惰性计算，不依赖该线程） |

## 代码布局

```
featforge/
  config.py       配置与门槛
  transforms.py   变换版本注册表（sum/mean/max/... + 类别型）与偏差度量
  db.py           SQLite schema（元数据/事件/订正台账/在线/离线/检查点/审计/
                  仲裁器/容量凭证与预算账本）
  store.py        领域核心：定义、写入、订正、回填、重算、校验、切换、回滚
  budget.py       容量凭证与预算结算：时间补充/突发上限、原子预留、延后队列
                  自动唤醒、单调结算与超额暂停、失联接替 fencing、终态退款
  arbiter.py      多租户仲裁器：统一 ready set、权重/席位/vruntime、欠额+老化、
                  claim_epoch fencing、心跳续期、游标进度、暂停恢复、仲裁历史
  arbiter_worker.py 无状态分片执行器（claim→逐分片进度→心跳→complete，故障注入）
  arbiter_bridge.py 既有 jobs 队列接入统一 ready set 的 opt-in 桥接层
  worker.py       容器化执行器（claim → run → 校验 → 自动切换）
  api.py          HTTP API + HTML 状态页
  __main__.py     api | worker | all 入口
  snapshot/       独立特征快照搬运模块（零第三方依赖，可独立部署）
    codec.py        规范化 JSON / sha256 / schema 标识 / 参数指纹
    chunking.py     确定性 JSONL 编码与固定块序切分
    manifest.py     内容寻址 manifest 的组装与校验
    blobstore.py    内容寻址对象存储（不可变块 + 原子物化）
    atomicfs.py     临时文件+rename 原子发布原语
    db.py           导出/导入元数据库（幂等键/bundle/transfer/chunks/active）
    exporter.py     导出服务（幂等 + fencing，中断不发布半成品）
    importer.py     导入服务（租约/检查点/三闸门/原子激活）
    executor.py     无状态块搬运执行器（多副本、崩溃续传）
    httpapi.py      标准库 HTTP 服务（exporter|importer 同构路由）
    client.py       HTTP 客户端
    source.py       物化来源：DemoFeatureStore / FeatForgeOfflineSource
    __main__.py     exporter | importer | worker | seed-db 入口
scripts/demo.py   端到端演示
scripts/arbiter_demo.py  多租户仲裁器 8 场景可复现演示（权重分流/防饥饿/席位/
                         暂停恢复/进程退出接替/旧 epoch 拒绝/检查点继续/业务键去重）
scripts/snapshot_demo.py  快照搬运 8 场景可复现演示（两个真实进程）
scripts/budget_demo.py  容量凭证与预算 7 场景可复现演示（预留结算/延后自动唤醒/
                         并发抢额防透支/超额暂停/失联接替/终止幂等/完成-终止竞争）
tests/            142 个测试（PIT、检查点、迟到、代际门槛、回滚、并发、
                  订正台账、崩溃恢复、读者一致性、时间旅行/跨代差异、HTTP、
                  多租户仲裁器 35 个、快照搬运 21 个、容量凭证与预算 25 个）
```

> 说明：存储默认用 SQLite + WAL 以零依赖完整表达事务语义；`Store` 是数据库无关的
> 领域层，生产可将 `db.py` 替换为 Postgres/数仓连接而不变更业务规则。
