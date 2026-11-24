import json
from contextlib import closing
from dataclasses import replace

import verified_fixture
from catalog.replicas import initialize_replica_store, prepare, get_replica, read_replica_group
from catalog.verified import encode_proof, parse_credential
from catalog.verified_control import initialize_control, load_salts
from lake_db import open_database
from publication.history import read
from publication.signing import IncompleteCoverage, sign
from receiving.coverage import InvalidEvidence, get_confirmed_snapshot
from receiving.intake import StaleAssignment, receive, receive_verified
from receiving.receipts import list_accepted_segments
from receiving.requests import (find_assignment, get_receiving_request, register_assignment,
                                register_request, register_verified)
from scan.handoff import begin_handoff
from scan.requests import get_request, request, request_verified
from scan.segments import VerifiedSegment, reseal, scan, scan_verified
from scan.tasks import assign_task, get_task, record_task_state
from tests.fixture import LakeCase, details_view, make_rows


class VerifiedCase(LakeCase):
    def setUp(self):
        super().setUp()
        self.control_path = self.directory / 'control.sqlite'

    def open_control(self):
        path = getattr(self, 'control_path')
        return self.stack.enter_context(closing(open_database(path)))

    def prepare_f(self, replicas=None):
        self.revision_id = verified_fixture.populate(self.source, self.primary, self.standby)
        self.control = self.open_control()
        self.credential = prepare(self.source, self.control, self.revision_id,
                                  replicas or (('primary', self.primary), ('standby', self.standby)))
        return self.credential

    def start(self, request_id='night', start=0, stop=3, flag='可用', replica='primary'):
        obligation = request_verified(self.control, self.revision_id, request_id, start, stop, flag)
        task = assign_task(self.control, request_id, replica, f'{request_id}-primary').assignment
        register_verified(self.inbox, obligation, task, self.credential)
        return task


class VerifiedFlowTests(VerifiedCase):
    def test_v11_offline_flow_rejects_omitted_hit_and_signs_eighteen(self):
        self.prepare_f()
        self.source.close()
        task = self.start()
        first = scan_verified(self.control, self.primary, task.task_id, 0)
        self.assertEqual(first.segment.checked_positions, (0, 1))
        first_receipt = receive_verified(self.inbox, 'night', first)
        record_task_state(self.control, task.task_id, 'lost')
        successor = begin_handoff(self.control, 'night', task.task_id, 'standby', 'night-standby')
        register_assignment(self.inbox, successor.assignment)
        standby = successor.assignment.task_id

        legal = scan_verified(self.control, self.standby, standby, 1)
        self.assertEqual(legal.segment.checked_positions, (1, 2))
        self.assertEqual(details_view(legal.segment.hits), ((2, 31, '南闸/C', 11),))

        # Same correct task seal, but position 2 is silently dropped from the hits.
        replica = get_replica(self.control, self.revision_id, 'standby')
        group = read_replica_group(self.standby, replica, 1)
        omitted = replace(legal.segment, hits=())
        forged = VerifiedSegment(reseal(get_task(self.control, standby).assignment, omitted),
                                 encode_proof(self.revision_id, get_request(self.control, 'night'),
                                              omitted, group.rows, load_salts(self.control, self.revision_id)))
        with self.assertRaises(InvalidEvidence):
            receive_verified(self.inbox, 'night', forged)
        with self.assertRaises(IncompleteCoverage) as pending:
            sign(self.inbox, 'night')
        self.assertEqual(pending.exception.missing_positions, (2,))
        self.assertEqual(self.write_counts(), (0, 0))
        self.assertEqual(get_confirmed_snapshot(self.inbox, 'night').checked_positions, frozenset((0, 1)))

        # The untouched primary segment replays its original receipt.
        self.assertEqual(receive_verified(self.inbox, 'night', first), first_receipt)
        tail_receipt = receive_verified(self.inbox, 'night', legal)
        result = sign(self.inbox, 'night')
        self.assertEqual(details_view(result.details), ((0, 40, '北闸/A', 7), (2, 31, '南闸/C', 11)))
        self.assertEqual(result.total_liters, 18)
        self.assertEqual(receive_verified(self.inbox, 'night', legal), tail_receipt)
        self.assertEqual(receive_verified(self.inbox, 'night', first), first_receipt)

        # After the control and standby are gone, only the isolation store remains.
        self.primary.close()
        self.standby.close()
        self.control.close()
        self.reopen_receiving()
        self.assertEqual(read(self.inbox, result.result_id), result)
        self.assertEqual(receive_verified(self.inbox, 'night', legal), tail_receipt)
        self.assertEqual(get_receiving_request(self.inbox, 'night').active_task_id, standby)

    def test_v11_late_requests_share_one_credential(self):
        self.prepare_f()
        self.source.close()
        for request_id, flag, expected, total in (
                ('window-clear', '可用', (), 0),
                ('window-pending', '待核', ((1, 12, '内务/B', 91),), 91)):
            task = self.start(request_id, 1, 2, flag, replica='primary')
            segment = scan_verified(self.control, self.primary, task.task_id, 0)
            self.assertEqual(segment.segment.checked_positions, (1,))
            receive_verified(self.inbox, request_id, segment)
            result = sign(self.inbox, request_id)
            self.assertEqual(details_view(result.details), expected)
            self.assertEqual(result.total_liters, total)
        self.assertEqual(self.write_counts(), (2, 2))

    def test_v11_stale_task_cannot_add_new_coverage(self):
        self.prepare_f()
        task = self.start()
        late = scan_verified(self.control, self.primary, task.task_id, 1)
        first = scan_verified(self.control, self.primary, task.task_id, 0)
        receive_verified(self.inbox, 'night', first)
        record_task_state(self.control, task.task_id, 'lost')
        successor = begin_handoff(self.control, 'night', task.task_id, 'standby', 'night-standby')
        register_assignment(self.inbox, successor.assignment)
        with self.assertRaises(StaleAssignment):
            receive_verified(self.inbox, 'night', late)
        self.assertEqual(get_confirmed_snapshot(self.inbox, 'night').checked_positions, frozenset((0, 1)))
        # A lost old task and a replaced task both replay their confirmed segments.
        self.assertEqual(receive_verified(self.inbox, 'night', first).request_id, 'night')

    def test_v11_modes_do_not_cross(self):
        self.prepare_f()
        task = self.start()
        obligation = request(self.source, self.revision_id, 'plain', 0, 3, '可用')
        assignment = assign_task(self.source, 'plain', 'primary', 'plain-task').assignment
        register_request(self.inbox, obligation, assignment)
        ordinary = scan(self.source, self.primary, 'plain-task', 0)
        verified = scan_verified(self.control, self.primary, task.task_id, 0)
        for request_id, evidence, handler in (
                ('night', ordinary, receive_verified),
                ('plain', verified, receive_verified),
                ('plain', verified, receive),
                ('night', verified, receive)):
            with self.subTest(request_id=request_id, handler=handler.__name__):
                with self.assertRaises(InvalidEvidence):
                    handler(self.inbox, request_id, evidence)
        with self.assertRaises(ValueError):
            register_verified(self.inbox, obligation, assignment, self.credential)

    def test_v11_bad_evidence_variants_are_rejected(self):
        self.prepare_f()
        task = self.start()
        segment = scan_verified(self.control, self.primary, task.task_id, 0)
        first = receive_verified(self.inbox, 'night', segment)
        snapshot = get_confirmed_snapshot(self.inbox, 'night')
        forged = [
            replace(segment, proof=b''),
            replace(segment, proof=b'not-a-proof'),
            replace(segment, proof=segment.proof + b' '),
            replace(segment, segment=replace(segment.segment, checked_positions=(0,))),
            replace(segment, segment=replace(segment.segment, hits=())),
            replace(segment, segment=replace(segment.segment, group_number=1)),
            replace(segment, segment=replace(segment.segment, revision_id='G')),
            replace(segment, segment=replace(segment.segment, request_id='other')),
            VerifiedSegment(segment.segment, segment.proof[:-1]),
        ]
        for bad in forged:
            with self.subTest(bad=bad):
                with self.assertRaises(InvalidEvidence):
                    receive_verified(self.inbox, 'night', bad)
        self.assertEqual(get_confirmed_snapshot(self.inbox, 'night'), snapshot)
        self.assertEqual(tuple(s.receipt for s in list_accepted_segments(self.inbox, 'night')), (first,))

    def test_v11_wrong_group_credential_and_replica(self):
        self.prepare_f(replicas=(('primary', self.primary), ('standby', self.standby)))
        obligation = request_verified(self.control, self.revision_id, 'night', 0, 3, '可用')
        # A task for an unprepared replica is refused on registration.
        other = self.stack.enter_context(closing(open_database(':memory:')))
        initialize_replica_store(other)
        self.assertEqual(other.execute('SELECT count(*) FROM replica_identity').fetchone()[0], 0)
        task = assign_task(self.control, 'night', 'primary', 'night-primary').assignment
        with self.assertRaises(ValueError):
            register_verified(self.inbox, obligation, replace(task, replica_id='absent'), self.credential)
        with self.assertRaises(ValueError):
            register_verified(self.inbox, replace(obligation, revision_id='G'), task, self.credential)
        with self.assertRaises(ValueError):
            register_verified(self.inbox, replace(obligation, stop=4), task, self.credential)
        with self.assertRaises(ValueError):
            register_verified(self.inbox, obligation, task, b'not a credential')
        register_verified(self.inbox, obligation, task, self.credential)
        for group in (2, 3, -1, '0'):
            with self.subTest(group=group):
                with self.assertRaises((ValueError, LookupError)):
                    scan_verified(self.control, self.primary, task.task_id, group)


class VerifiedBindingTests(VerifiedCase):
    def test_v11_credential_binding_is_stable(self):
        credential = self.prepare_f()
        task = self.start()
        again = register_verified(self.inbox, get_receiving_request(self.inbox, 'night').request,
                                  find_assignment(self.inbox, task.task_id), credential)
        self.assertEqual(again.active_task_id, task.task_id)
        with self.assertRaises(ValueError):
            register_request(self.inbox, get_receiving_request(self.inbox, 'night').request, task)
        with self.assertRaises(ValueError):
            register_verified(self.inbox, get_receiving_request(self.inbox, 'night').request,
                              find_assignment(self.inbox, task.task_id),
                              replace_credential(credential))
        self.reopen_receiving()
        self.assertTrue(get_receiving_request(self.inbox, 'night').verified)
        self.assertEqual(register_verified(self.inbox, get_receiving_request(self.inbox, 'night').request,
                                           find_assignment(self.inbox, task.task_id), credential).active_task_id,
                         task.task_id)

    def test_v11_control_state_survives_reopen(self):
        self.prepare_f()
        task = self.start()
        first = scan_verified(self.control, self.primary, task.task_id, 0)
        receipt = receive_verified(self.inbox, 'night', first)
        self.control.close()
        self.control = self.open_control()
        self.assertEqual(get_receiving_request(self.inbox, 'night').active_task_id, task.task_id)
        self.assertEqual(receive_verified(self.inbox, 'night', first), receipt)
        self.assertEqual(scan_verified(self.control, self.primary, task.task_id, 1).segment.checked_positions,
                         (2,))

    def test_v11_credential_does_not_disclose_hidden_facts(self):
        credential = self.prepare_f()
        blob = json.loads(credential)
        self.assertEqual(blob['length'], 3)
        self.assertEqual(blob['batch_id'], 'F')
        text = credential.decode('utf-8')
        for secret in ('北闸/A', '内务/B', '南闸/C', '"7"', '"91"', '"11"', '盐'):
            self.assertNotIn(secret, text)
        task = self.start()
        segment = scan_verified(self.control, self.primary, task.task_id, 0)
        proof = json.loads(segment.proof)
        for opening in proof['openings']:
            if not opening['hit']:
                self.assertEqual(set(opening), {'position', 'hit', 'quality_salt', 'quality'})
        # A non-hit opening cannot be tested against the withheld values.
        non_hits = [opening for opening in proof['openings'] if not opening['hit']]
        self.assertTrue(non_hits)
        for opening in non_hits:
            self.assertNotIn('liters', opening)
            self.assertNotIn('origin', opening)

    def test_v11_prepare_guards(self):
        self.revision_id = verified_fixture.populate(self.source, self.primary, self.standby)
        control = self.open_control()
        with self.assertRaises(ValueError):
            prepare(self.source, control, self.revision_id, ())
        with self.assertRaises(LookupError):
            prepare(self.source, control, self.revision_id, (('absent', self.primary),))
        with self.assertRaises(ValueError):
            prepare(self.source, control, self.revision_id, (('primary', self.primary),) * 2)
        with self.assertRaises(ValueError):
            prepare(self.source, self.source, self.revision_id, (('primary', self.primary),))
        with self.assertRaises(LookupError):
            prepare(self.source, control, 'G', (('primary', self.primary),))
        credential = prepare(self.source, control, self.revision_id,
                             (('primary', self.primary), ('standby', self.standby)))
        self.assertEqual(parse_credential(credential).length, 3)
        with self.assertRaises(ValueError):
            prepare(self.source, control, self.revision_id,
                    (('primary', self.primary), ('standby', self.standby)))
        initialize_control(control)
        with self.assertRaises(LookupError):
            request_verified(control, 'G', 'x', 0, 1, '可用')
        with self.assertRaises(ValueError):
            request_verified(control, self.revision_id, 'x', 0, 4, '可用')

    def test_v11_zero_hit_request_still_proves_coverage(self):
        revision = self.frozen('H', make_rows('H', (7, 9, 11), ('待核', '待核', '待核')))
        self.load(revision, 'main', self.primary, (0, 1), (2,))
        control = self.open_control()
        credential = prepare(self.source, control, revision.revision_id,
                             (('main', self.primary),))
        self.source.close()
        obligation = request_verified(control, revision.revision_id, 'night', 0, 3, '可用')
        task = assign_task(control, 'night', 'main', 'night-main').assignment
        register_verified(self.inbox, obligation, task, credential)
        for number in (0, 1):
            segment = scan_verified(control, self.primary, task.task_id, number)
            self.assertEqual(segment.segment.hits, ())
            receive_verified(self.inbox, 'night', segment)
        result = sign(self.inbox, 'night')
        self.assertEqual((result.details, result.total_liters), ((), 0))
        self.assertEqual(scan_verified(control, self.primary, task.task_id, 0) is None, False)

    def test_v11_missing_value_listed_without_counting(self):
        revision = self.frozen('M', make_rows('M', (None, 0, 4)))
        self.load(revision, 'main', self.primary, (0, 1, 2),)
        control = self.open_control()
        credential = prepare(self.source, control, revision.revision_id, (('main', self.primary),))
        self.source.close()
        obligation = request_verified(control, revision.revision_id, 'night', 0, 2, '可用')
        task = assign_task(control, 'night', 'main', 'night-main').assignment
        register_verified(self.inbox, obligation, task, credential)
        receive_verified(self.inbox, 'night', scan_verified(control, self.primary, task.task_id, 0))
        result = sign(self.inbox, 'night')
        self.assertEqual(details_view(result.details),
                         ((0, 0, '站点/M/0', None), (1, 1, '站点/M/1', 0)))
        self.assertIs(type(result.details[1].liters), int)
        self.assertEqual(result.total_liters, 0)

    def test_v11_tampered_replica_group_is_refused(self):
        self.prepare_f()
        task = self.start()
        self.assertEqual(scan_verified(self.control, self.primary, task.task_id, 0)
                         .segment.checked_positions, (0, 1))
        self.standby.execute('''
            UPDATE replica_groups SET body = replace(body, '南闸/C', '南闸/X')
            WHERE replica_id = 'standby' AND group_number = 1
        ''')
        second = request_verified(self.control, self.revision_id, 'second', 0, 3, '可用')
        successor = assign_task(self.control, 'second', 'standby', 'second-primary').assignment
        register_verified(self.inbox, second, successor, self.credential)
        with self.assertRaises(ValueError):
            scan_verified(self.control, self.standby, 'second-primary', 1)
        # The primary replica remains independently readable and verifiable.
        self.assertEqual(scan_verified(self.control, self.primary, task.task_id, 1)
                         .segment.checked_positions, (2,))


def replace_credential(credential):
    """A syntactically valid but different credential (different commitments)."""
    payload = json.loads(credential)
    payload['commitments'][0]['value'] = '0' * 64
    return json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
