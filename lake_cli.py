import argparse
import json
from contextlib import ExitStack, closing
from dataclasses import asdict, replace
from pathlib import Path
from tempfile import TemporaryDirectory

from audit.checks import audit
from audit.exports import format_audit_record
from audit.ledger import initialize_audit_ledger
from catalog.freeze import freeze
from catalog.layouts import ReplicaLayout
from catalog.replicas import (get_replica, initialize_replica_catalog, initialize_replica_store,
                              prepare, read_replica_group, replicate)
from catalog.revisions import FrozenRow, get_revision, initialize_revisions
from catalog.verified import encode_proof
from catalog.verified_control import initialize_control, load_salts
from intake.batches import ingest, initialize_batch_store
from intake.rows import ObservationRow
from lake_db import open_database
from publication.history import read
from publication.signing import IncompleteCoverage, sign
from publication.store import initialize_publications
from receiving.coverage import InvalidEvidence
from receiving.intake import StaleAssignment, receive, receive_verified
from receiving.receipts import initialize_segment_receipts
from receiving.requests import (initialize_receiving_requests, register_assignment, register_request,
                                register_verified)
from scan.handoff import begin_handoff
from scan.requests import initialize_scan_requests, request, request_verified
from scan.segments import VerifiedSegment, reseal, scan, scan_verified
from scan.tasks import assign_task, get_task, initialize_scan_tasks, record_task_state


SAMPLES = Path(__file__).with_name('samples')


def initialize_source(db):
    initialize_batch_store(db)
    initialize_revisions(db)
    initialize_replica_catalog(db)
    initialize_scan_requests(db)
    initialize_scan_tasks(db)


def initialize_receiving(db):
    initialize_receiving_requests(db)
    initialize_segment_receipts(db)
    initialize_publications(db)
    initialize_audit_ledger(db)


def initialize_directory(directory):
    directory.mkdir(parents=True, exist_ok=True)
    for name, initialize in (('source', initialize_source), ('primary', initialize_replica_store),
                             ('standby', initialize_replica_store), ('control', initialize_control),
                             ('receiving', initialize_receiving)):
        with closing(open_database(directory / f'{name}.sqlite')) as db:
            initialize(db)


def _sample(name):
    return json.loads((SAMPLES / name).read_text(encoding='utf-8'))


def demo(directory):
    source_sample = _sample('frozen_f.json')
    review_sample = _sample('review_f.json')
    initialize_directory(directory)
    receiving_path = directory / 'receiving.sqlite'
    with ExitStack() as stack:
        db = stack.enter_context(closing(open_database(directory / 'source.sqlite')))
        primary = stack.enter_context(closing(open_database(directory / 'primary.sqlite')))
        standby = stack.enter_context(closing(open_database(directory / 'standby.sqlite')))
        inbox = stack.enter_context(closing(open_database(receiving_path)))
        batch_id = source_sample['batch_id']
        ingest(db, batch_id, (ObservationRow(**row) for row in source_sample['rows']))
        revision_id = freeze(db, batch_id)
        revision = get_revision(db, revision_id)
        for name, replica_db in (('primary', primary), ('standby', standby)):
            layout = ReplicaLayout.from_positions(revision, enumerate(source_sample[f'{name}_groups']))
            replicate(db, replica_db, revision_id, name, layout)
        request_id = source_sample['request_id']
        obligation = request(db, revision_id, request_id, source_sample['start'], source_sample['stop'],
                             source_sample['quality'])
        task = assign_task(db, request_id, 'primary', 'day-primary')
        register_request(inbox, obligation, task.assignment)
        first_segment = scan(db, primary, 'day-primary', 0)
        first_receipt = receive(inbox, request_id, first_segment)
        late = scan(db, primary, 'day-primary', 1)
        record_task_state(db, 'day-primary', 'lost')
        successor = begin_handoff(db, request_id, 'day-primary', 'standby', 'day-standby')
        register_assignment(inbox, successor.assignment)
        receive(inbox, request_id, scan(db, standby, 'day-standby', 1))
        try:
            premature = sign(inbox, request_id)
        except IncompleteCoverage as pending:
            if pending.missing_positions != (3,):
                raise RuntimeError(f'错误的待查位置 {pending.missing_positions}') from pending
        else:
            raise RuntimeError(f'全范围实收前签发 {premature.total_liters} L')
        try:
            receive(inbox, request_id, late)
        except StaleAssignment:
            pass
        receive(inbox, request_id, scan(db, standby, 'day-standby', 2))
        retained = receive(inbox, request_id, first_segment)
        signed = sign(inbox, request_id)

    with closing(open_database(receiving_path)) as inbox:
        collected = read(inbox, signed.result_id)
        record = audit(inbox, collected.result_id, (FrozenRow(**row) for row in review_sample['details']),
                       review_sample['total_liters'])
    expected = {'details': review_sample['details'], 'total_liters': review_sample['total_liters']}
    actual = {'details': [asdict(row) for row in signed.details], 'total_liters': signed.total_liters}
    downstream_expected = {'same_publication': True, 'same_first_receipt': True, 'same_issue_receipt': True,
                           'row_count': len(review_sample['details']), 'total_liters': review_sample['total_liters']}
    downstream_actual = {'same_publication': collected == signed, 'same_first_receipt': retained == first_receipt,
                         'same_issue_receipt': record.receipt == signed.receipt,
                         'row_count': record.row_count, 'total_liters': record.total_liters}
    if actual != expected or downstream_actual != downstream_expected:
        raise RuntimeError(f'抽查或核账不符 {actual!r} {downstream_actual!r}')
    print(format_audit_record(record))
    print('POINTWISE_SCENARIO ' + json.dumps({
        'case_id': 'v10', 'input': source_sample,
        'expected': expected, 'actual': actual,
        'downstream_expected': downstream_expected, 'downstream_actual': downstream_actual,
        'code_paths': ['scan/segments.py', 'receiving/intake.py', 'receiving/coverage.py',
                       'publication/signing.py', 'audit/checks.py'],
        'oracle': '手查样例位置 0、2、3 的标志和来源，按 7 + 11 + 7 计算，断源后比对原成果与原回执。'
    }, ensure_ascii=False, separators=(',', ':')))


def demo_verified(directory):
    """F-batch verified sampling: prepare, shut the source, reject an omission, sign 18 L.

    Only the isolation store is reopened for retrieval and review at the end.
    """
    from verified_fixture import populate

    initialize_directory(directory)
    receiving_path = directory / 'receiving.sqlite'
    with ExitStack() as stack:
        source = stack.enter_context(closing(open_database(directory / 'source.sqlite')))
        primary = stack.enter_context(closing(open_database(directory / 'primary.sqlite')))
        standby = stack.enter_context(closing(open_database(directory / 'standby.sqlite')))
        control = stack.enter_context(closing(open_database(directory / 'control.sqlite')))
        inbox = stack.enter_context(closing(open_database(receiving_path)))

        revision_id = populate(source, primary, standby)
        credential = prepare(source, control, revision_id,
                             (('primary', primary), ('standby', standby)))
        # The trusted credential is handed to the isolation side before shutdown.
        source.close()

        request_id = 'night-verified'
        obligation = request_verified(control, revision_id, request_id, 0, 3, '可用')
        primary_task = assign_task(control, request_id, 'primary', 'verified-primary').assignment
        register_verified(inbox, obligation, primary_task, credential)
        first = scan_verified(control, primary, 'verified-primary', 0)
        first_receipt = receive_verified(inbox, request_id, first)

        record_task_state(control, 'verified-primary', 'lost')
        successor = begin_handoff(control, request_id, 'verified-primary', 'standby', 'verified-standby')
        register_assignment(inbox, successor.assignment)
        standby_task = successor.assignment.task_id
        legal = scan_verified(control, standby, standby_task, 1)

        # A correctly sealed standby segment under the registered task that reports
        # checking positions 1 and 2 but silently omits the hit at position 2.
        replica = get_replica(control, revision_id, 'standby')
        group = read_replica_group(standby, replica, 1)
        doctored = replace(legal.segment, hits=())
        forged = VerifiedSegment(
            reseal(get_task(control, standby_task).assignment, doctored),
            encode_proof(revision_id, obligation, doctored, group.rows, load_salts(control, revision_id)))
        try:
            receive_verified(inbox, request_id, forged)
        except InvalidEvidence:
            rejected = True
        else:
            raise RuntimeError('漏列的备用段被接纳')
        pending = None
        try:
            sign(inbox, request_id)
        except IncompleteCoverage as remaining:
            pending = remaining.missing_positions
        if pending != (2,):
            raise RuntimeError(f'漏列后待查位置错误 {pending!r}')
        retained = receive_verified(inbox, request_id, first) == first_receipt
        receive_verified(inbox, request_id, legal)
        signed = sign(inbox, request_id)

        # A later request on the same batch credential, under each quality flag.
        late = {}
        for request_id_late, flag in (('late-clear', '可用'), ('late-pending', '待核')):
            late_obligation = request_verified(control, revision_id, request_id_late, 1, 2, flag)
            task = assign_task(control, request_id_late, 'standby', f'{request_id_late}-task').assignment
            register_verified(inbox, late_obligation, task, credential)
            receive_verified(inbox, request_id_late,
                             scan_verified(control, standby, task.task_id, 1))
            late[request_id_late] = sign(inbox, request_id_late)

        control.close()
        primary.close()
        standby.close()

    expected_details = [
        {'position': 0, 'original_position': 40, 'origin': '北闸/A', 'liters': 7, 'quality': '可用'},
        {'position': 2, 'original_position': 31, 'origin': '南闸/C', 'liters': 11, 'quality': '可用'},
    ]
    # Only the isolation store is reopened: retrieve the original publication and receipt.
    with closing(open_database(receiving_path)) as reopened:
        collected = read(reopened, signed.result_id)
        replay = receive_verified(reopened, request_id, legal)
        late_replay = read(reopened, late['late-pending'].result_id)
        record = audit(reopened, collected.result_id,
                       (FrozenRow(**row) for row in expected_details), 18)

    actual_details = [asdict(row) for row in signed.details]
    if (actual_details, signed.total_liters) != (expected_details, 18):
        raise RuntimeError(f'可验抽查明细或升数不符 {actual_details!r} {signed.total_liters} L')
    if not (collected == signed and retained and replay.receipt_id and
            (collected.receipt == signed.receipt) and record.receipt == signed.receipt
            and late_replay == late['late-pending']):
        raise RuntimeError('断源后的原成果、补投回执或迟到请求不符')
    if (late['late-clear'].details, late['late-clear'].total_liters) != ((), 0):
        raise RuntimeError('可用标志下的窗口应为 0 L')
    if late['late-pending'].total_liters != 91:
        raise RuntimeError('待核标志下的窗口应为 91 L')

    print(format_audit_record(record))
    print('VERIFIED_SCENARIO ' + json.dumps({
        'case_id': 'v11',
        'credential_bytes': len(credential),
        'omission_rejected': rejected,
        'pending_after_rejection': list(pending),
        'primary_receipt_retained': retained,
        'details': actual_details,
        'total_liters': signed.total_liters,
        'late_clear_total': late['late-clear'].total_liters,
        'late_pending_total': late['late-pending'].total_liters,
        'offline_same_publication': collected == signed,
        'offline_same_first_receipt': collected.receipt == signed.receipt,
        'oracle': '按冻结逻辑序核位置 0、2 的原行，7 + 11 = 18 L；漏列位置 2 的备用段整段拒收。',
    }, ensure_ascii=False, separators=(',', ':')))


def main():
    parser = argparse.ArgumentParser(description='青沅观测湖抽查台账')
    commands = parser.add_subparsers(dest='command', required=True)
    initialize = commands.add_parser('init', help='初始化或重开已有业务库')
    initialize.add_argument('directory', type=Path)
    run = commands.add_parser('demo', help='执行固定日批的接替、签发及断源核账')
    run.add_argument('--directory', type=Path, help='新建目录以保留本次演示数据库')
    verified = commands.add_parser('verified-demo', help='演示可验抽查的漏列拒收、补齐与断源领取')
    verified.add_argument('--directory', type=Path, help='新建目录以保留本次演示数据库')
    args = parser.parse_args()
    if args.command == 'init':
        initialize_directory(args.directory)
    elif args.command == 'demo':
        if args.directory is not None:
            args.directory.mkdir()
            demo(args.directory)
        else:
            with TemporaryDirectory(prefix='qingyuan-demo-') as temporary:
                demo(Path(temporary))
    elif args.directory is not None:
        args.directory.mkdir()
        demo_verified(args.directory)
    else:
        with TemporaryDirectory(prefix='qingyuan-verified-') as temporary:
            demo_verified(Path(temporary))


if __name__ == '__main__':
    main()
