# 青沅观测湖抽查台账

站点向青沅流域资料中心提交按原序排列的计量观测。资料员固定日批并装载主备副本，夜班检查调度单指定的范围，次班凭签发件核对交班账。备用副本可以用不同的物理分组。隔离收件库独立判断实际检查范围，断开资料库和副本后仍可领取签发件和核账。

## 运行

需要 Python 3.11 和标准库。在由平台物化的项目根目录运行完整回归和演示。

```sh
python3 -B check_project.py
python3 -B verify_project.py
python3 -B -m lake_cli demo
python3 -B -m lake_cli verified-demo
```

`verify_project.py` 先跑完整原回归，再跑可验抽查的业务核验，任一失败以非零退出。演示在临时数据库中装载 `samples/frozen_f.json`，签发后用 `samples/review_f.json` 独立核对。可用 `python3 -B -m lake_cli demo --directory ./demo-run` 在一个新目录保留本次结果。`python3 -B -m lake_cli init ./lake-data` 为业务目录建缺失的表，可反复执行并保留原数据。演示目录和业务目录分别使用 `source.sqlite`、`primary.sqlite`、`standby.sqlite`、`control.sqlite`、`receiving.sqlite`。写入API各自拥有业务事务，在调用者事务之外调用。低层保存函数由业务事务调用。用 `lake_db.open_database` 打开连接并启用外键。

## 资料接纳和副本装载

1. `intake.batches.ingest(source, batch_id, rows)` 接纳有序 `intake.rows.ObservationRow(position, origin, liters, quality)`。原位置为批内唯一的非负整数，可以与输入顺序不同。出处为非空文本，升值为整数或 `None`，质控标志为 `可用` 或 `待核`。非法批次抛 `ValueError` 并整批拒绝。同号同参返回原接纳记录及回执，同号异参拒绝。`get_accepted_batch` 和 `intake.receipts.get_import_receipt` 可复查原序和出处。
2. `catalog.freeze.freeze(source, batch_id)` 返回不可变冻结号，沿用批号作为冻结命名空间中的标识。`catalog.revisions.get_revision` 取回冻结目录。未接纳的批号抛 `LookupError`。冻结位置按输入顺序从零编号，原位置随行保留。
3. `catalog.replicas.replicate(source, replica_db, revision_id, replica_id, layout)` 在独立副本库保存组内容，完整提交以后在资料侧登记可用。`catalog.layouts.ReplicaLayout.from_positions(revision, numbered_positions)` 可从固定目录构造分组，也可直接提交 `LayoutGroup` 组成的布局。每个冻结位置必须恰出现一次且行内容一致。物理组号为布局内部唯一的非负 SQLite 整数，组内次序可以不同。错布局抛 `ValueError`，既有可用副本保持有效。两个库分别提交，资料侧登记失败时可用原参数补办。`get_replica` 查询可用副本，`read_replica_group` 读实际分组并用已登记摘要核对正文。

## 抽查、接替和签发

4. `scan.requests.request(source, revision_id, request_id, start, stop, quality)` 登记半开范围 `[start, stop)` 和等值筛选标志。范围须非空并位于冻结目录内。`scan.tasks.assign_task(source, request_id, replica_id, task_id)` 派给同批可用副本。调用方经可信交接通道把请求和 `task.assignment` 交给 `receiving.requests.register_request(inbox, obligation, task.assignment)`，其中包括用于核验出件载荷的派工密钥。
5. `scan.segments.scan(source, replica_db, task_id, group_number)` 读取整组并对请求内的行逐条检查。组与请求无交集时返回 `None`。其余情况下返回含检查位置、命中行和完整性凭据的 `SegmentEvidence`，即使命中为空。`receiving.intake.receive(inbox, request_id, evidence)` 验证出件并原子实收，返回 `SegmentReceipt`。错批、错派工、载荷失配或重叠事实冲突抛 `receiving.coverage.InvalidEvidence`。主任务失联时用 `record_task_state(source, task_id, 'lost')` 标记，再用 `scan.handoff.begin_handoff(source, request_id, task_id, standby_replica_id, new_task_id)` 派出备用，并以 `receiving.requests.register_assignment(inbox, successor.assignment)` 承接。登记成功即切换新增实收资格。旧任务未获确认的迟投抛 `StaleAssignment`，已经确认的相同出件仍返回原回执。备用可重扫全范围，也可投递能补齐已确认缺口的组。
6. `publication.signing.sign(inbox, request_id)` 只有在实收覆盖所有请求位置时才提交。覆盖不足抛 `IncompleteCoverage` 并带缺失位置。签发明细按逻辑位置升序排列，同值异位各出一行。命中的 `None` 保留空值并跳过合计，命中的 `0` 保留为 0 L，零命中合计 0 L。整数在持久层使用无损文本表示。成果、明细、合计和原签发回执同事务提交，失败可以凭原实收重试，已经签发的请求可领回同一成果。

## 次班复核

7. `publication.history.read(inbox, result_id)` 按成果号读取保存的正文和原签发回执，未知成果抛 `LookupError`。领取只需隔离库。
8. `audit.checks.audit(inbox, result_id, details, reported_total)` 用固定正文核对所交的 `FrozenRow` 序列，复算净量并保存引用原首执的成功记录。行顺序、原位置、数值、缺测、标志、逐位置出处或合计有误时抛 `AuditMismatch`，其 `differences` 提供字段和适用位置。`audit.ledger.get_audit_record` 可按核账号查回成功记录。

演示批 F 的主布局为 `[0,2)`、`[2,4)`，备用布局为 `[0,1)`、`[1,3)`、`[3,4)`。夜班先收到主组 0，再收到备用组 1。此时仍缺逻辑位置 3。完整成果列出位置 0、2、3 及原出处，合计 `7 + 11 + 7 = 25 L`。回归另覆盖错批、空命中、真实提交回滚、重复初始化及断源后复领。

## 冻结批次可验抽查

资料库关停前后改用独立控制库和已装载副本，隔离库依旧独立收件、签发和领取。接口细节见 `VERIFIED_SAMPLING.md`。

1. `catalog.replicas.prepare(source, dispatch, revision_id, replicas)` 只为已登记、已装载的副本开通一次。控制库写入冻结身份、逻辑长度、逐位置加盐承诺、副本分组和非披露开采材料；凭据返回给资料员并在关库前交隔离侧保存。凭据与合法段都不含未命中位置的读值和出处、窗外原行或质控事实，隔离侧也无法凭候选值反查承诺。
2. `scan.requests.request_verified` 只用控制库登记窗口；`scan.tasks` 与 `scan.handoff` 仍办主任务和接替。`receiving.requests.register_verified` 以单独交来的凭据绑定隔离请求，重投保留原绑定，普通登记不能改写它。
3. `scan.segments.scan_verified` 先按登记摘要实读副本组、再核对控制库里的准备分组，然后按窗口出 `VerifiedSegment(segment, proof)`。`receiving.intake.receive_verified` 独立重算承诺，确认本段实际应查位置与每个命中判定后才收；错版、错行、越窗、额外事实、缺证坏证、普通段进入可验请求及失格旧任务均拒收且不留覆盖。已确认段重投（含断源、换任务后）领原回执。
4. 签发、领取和核账沿用 `publication.signing.sign`、`publication.history.read`、`audit.checks.audit`。运行 `python3 -B -m lake_cli verified-demo [--directory DIR]` 演示：开通后关资料库，主段确认、接替、漏列位置 2 的备用段整段拒收、未变动主段复领、合法备用段补齐后按冻结原序签出位置 0、2 共 18 L，以及只重开隔离库领取原成果和首执；迟到请求 `[1,2)` 在 `可用` 下签出 0 L、在 `待核` 下签出 91 L，两次共用同一凭据。
