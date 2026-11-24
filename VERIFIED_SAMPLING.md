# 冻结批次可验抽查

资料侧开通已冻结且已有登记副本的批次。一次交接的批次凭据固定确切冻结内容和逻辑长度，供后开的多个抽查使用，两个 V1 质控标志均可选。交接时尚未确定抽查范围或标志。关停资料库后，派工和扫描只用独立控制库和已准备的副本，不能重连资料库，也无需把冻结行正文复制进控制库。隔离库独立收件、签发和领取。

## 调用约定

连接沿用 `lake_db.open_database`。源库、副本和隔离库按原流程初始化，写入调用在调用者事务之外执行。

1. `catalog.replicas.prepare(source, dispatch, revision_id, replicas) -> bytes` 为一个已冻结批次开通可验抽查。`dispatch` 是独立、初始为空的控制库连接，`replicas` 是非空的 `(replica_id, replica_db)` 对序列，引用该冻结批次已登记、已装载的副本。开通将可信批次凭据返回给资料员，并为控制库及这些副本备妥下线后的派工与取证材料。凭据在关库前经可信途径交给隔离侧保存。只需支持一次成功开通供后续调用。
2. `scan.requests.request_verified(dispatch, revision_id, request_id, start, stop, quality) -> ScanRequest` 按冻结逻辑序登记非空半开窗口与 `可用` 或 `待核` 标志。控制库调用原 `assign_task`、`record_task_state` 和 `begin_handoff` 办主任务和接替。隔离库用 `receiving.requests.register_verified(inbox, obligation, primary_assignment, batch_credential) -> ReceivingRequest` 登记可验请求，使用单独交来的凭据。相同请求与主任务的重投须保持原凭据，普通登记或更换凭据不能改写已有绑定。接替用原 `register_assignment` 登记。
3. `scan.segments.scan_verified(dispatch, replica_db, task_id, group_number) -> VerifiedSegment | None` 先实读并核验常规副本组，再按指定窗口出段。无交集返回 `None`，有交集即使零命中也出可验段。`scan.segments.VerifiedSegment` 是可用 `dataclasses.replace` 替换字段的记录，`segment` 是原 `SegmentEvidence`，`proof` 是携带本段冻结依据的 `bytes`。凭据和冻结证据的内部格式由实现决定。
4. `receiving.intake.receive_verified(inbox, request_id, evidence) -> SegmentReceipt` 按已登记任务及可信凭据接纳。错版、错行、越窗、额外事实、缺证、坏证和普通段进入可验请求均抛 `InvalidEvidence`，失去资格的旧任务新增段沿用 `StaleAssignment`。上述拒收不产生回执或覆盖。同一可验段已确认时，重投领原回执，包括切换任务或重开后。请求模式、凭据绑定和已确认覆盖须在重开后保持。普通请求继续通过原入口收普通段。
5. 沿用 `publication.signing.sign`、`publication.history.read` 和 `audit.checks.audit`。签发所用覆盖只能来自已核定的实收。成果按冻结逻辑序列出原位置、升数、质控和出处。同值异位分别出行，缺测 `None` 保留且跳过求和，整数零保留。完整无命中出空明细和 0 L。未查齐抛原 `IncompleteCoverage`，同一成果重领原正文和首执。

## 每个检查位置能交出什么

隔离库须凭先交的批次依据核对本段所有报称已查的位置。每个位置上交的命中判定须符合已登记标志，命中原行须与冻结身份、逻辑位置、原位置、读值、质控和出处完全一致。完全未报称检查的位置不得计入覆盖。主备分组或组内次序不同仍认同一冻结逻辑位置。扫描方持有原任务封条所需的材料，任务封条本身只表明出件与登记派工相符。资料侧签发所用的私材和隔离侧专用核验秘密均不得交给扫描方。

凭据可以带批次身份、逻辑长度和不披露行内容的核验材料。合法段可证明自己检查的位置是否命中，只附命中的原行。未命中位置的读值和出处、请求范围之外的原行和质控事实不得随凭据、合法段或已接纳的隔离载荷披露。收到的依据也不得让隔离侧仅凭候选值与公开材料试猜并核对这些隐藏字段。隔离侧信任单独交来的批次凭据，并独立验证扫描来件。

## F 批抽查

`verified_fixture.populate(source, primary, standby)` 用 V1 API 接纳并装载样例，返回冻结号 `F`。按冻结顺序，位置 0 是北闸/A 的可用 7 L、原位置 40，位置 1 是内务/B 的待核 91 L、原位置 12，位置 2 是南闸/C 的可用 11 L、原位置 31。主副本组 0 的物理顺序为 1、0，组 1 是 2。备用副本组 0 是 0，组 1 的物理顺序为 2、1。

先开通，再关闭资料库，登记 `[0,3)` 的可用抽查。先确认主组 0，接替后收到备用组 1。若备用段报称查了 1、2 但漏列位置 2，即使持有正确任务封条，也须整段拒收。此时签发仍缺位置 2，未变动的主段可复领原实收。备用合法段补齐后签发位置 0、2 的原行，共 18 L。关闭控制库和副本，只重开隔离库即可领取并核账。另一个迟开的请求若只查 `[1,2)`，在可用标志下齐证签出 0 L，在待核标志下齐证签出该原行及 91 L。两次请求都用同一次开通的批次凭据。

## 核验与演示

保留 README 的普通抽查入口、原演示和完整回归。把可验抽查接入 `.pointwise-runtime.json`，沿用 `version` 为 1、`kind` 为 `cli` 的清单。`setup` 记录准备命令的参数数组列表，无准备步骤可为空。`verify` 和 `demo` 各记录一条参数数组。`verify` 运行完整原回归及可验抽查的业务核验，失败退出非零。`demo` 用 F 批演示漏列拒收、备用补齐后签出 18 L，及只重开隔离库领取原成果和首执。
