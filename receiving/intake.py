from lake_db import write_transaction
from catalog.verified import parse_credential, verify_proof
from receiving.coverage import InvalidEvidence, check_position_facts, get_confirmed_snapshot
from receiving.receipts import find_accepted_segment, store_accepted_segment
from receiving.requests import find_assignment, get_receiving_request
from receiving.verified import find_acceptance_proof, get_credential, store_acceptance
from scan.segments import SegmentEvidence, VerifiedSegment, verify_segment


class StaleAssignment(InvalidEvidence):
    pass


def receive(db, request_id, evidence):
    with write_transaction(db):
        entry = get_receiving_request(db, request_id)
        if entry.verified:
            raise InvalidEvidence('verified request requires verified segment evidence')
        if not isinstance(evidence, SegmentEvidence):
            raise InvalidEvidence('expected completed segment evidence')
        if (evidence.request_id, evidence.revision_id) != (request_id, entry.request.revision_id):
            raise InvalidEvidence('evidence belongs to a different request or frozen revision')
        if not isinstance(evidence.task_id, str):
            raise InvalidEvidence('invalid evidence task ID')
        assignment = find_assignment(db, evidence.task_id)
        if assignment is None or not verify_segment(assignment, evidence):
            raise InvalidEvidence('evidence does not match a registered scan assignment')
        previous = find_accepted_segment(db, request_id, evidence.task_id, evidence.group_number)
        if previous is not None:
            if previous.evidence != evidence:
                raise InvalidEvidence('confirmed segment has a different payload')
            return previous.receipt
        if evidence.task_id != entry.active_task_id:
            raise StaleAssignment('a replaced task cannot add new evidence')
        snapshot = get_confirmed_snapshot(db, request_id)
        check_position_facts(entry.request, snapshot, evidence)
        return store_accepted_segment(db, evidence)


def receive_verified(db, request_id, evidence):
    """Accept a verified segment only after independently checking its proof."""
    with write_transaction(db):
        entry = get_receiving_request(db, request_id)
        if not entry.verified:
            raise InvalidEvidence('ordinary request cannot accept verified evidence')
        if not isinstance(evidence, VerifiedSegment):
            raise InvalidEvidence('expected verified segment evidence')
        segment = evidence.segment
        if not isinstance(segment, SegmentEvidence):
            raise InvalidEvidence('verified segment has no completed evidence')
        if (segment.request_id, segment.revision_id) != (request_id, entry.request.revision_id):
            raise InvalidEvidence('evidence belongs to a different request or frozen revision')
        if not isinstance(segment.task_id, str):
            raise InvalidEvidence('invalid evidence task ID')
        assignment = find_assignment(db, segment.task_id)
        if assignment is None or not verify_segment(assignment, segment):
            raise InvalidEvidence('evidence does not match a registered scan assignment')
        try:
            credential = parse_credential(get_credential(db, request_id))
            if credential.revision_id != segment.revision_id:
                raise InvalidEvidence('batch credential belongs to a different frozen revision')
            verify_proof(credential, entry.request, segment, evidence.proof)
        except InvalidEvidence:
            raise
        except (LookupError, ValueError) as error:
            raise InvalidEvidence(str(error)) from error

        previous = find_accepted_segment(db, request_id, segment.task_id, segment.group_number)
        if previous is not None:
            if previous.evidence != segment:
                raise InvalidEvidence('confirmed segment has a different payload')
            if find_acceptance_proof(db, request_id, segment.task_id, segment.group_number) != evidence.proof:
                raise InvalidEvidence('confirmed segment has a different proof')
            return previous.receipt
        if segment.task_id != entry.active_task_id:
            raise StaleAssignment('a replaced task cannot add new evidence')
        snapshot = get_confirmed_snapshot(db, request_id)
        check_position_facts(entry.request, snapshot, segment)
        receipt = store_accepted_segment(db, segment)
        store_acceptance(db, receipt.receipt_id, request_id, segment.task_id,
                         segment.group_number, evidence.proof)
        return receipt
