from contextlib import ExitStack, closing

from catalog.freeze import freeze
from catalog.layouts import ReplicaLayout
from catalog.replicas import initialize_replica_store, replicate
from catalog.revisions import FrozenRow, get_revision
from intake.batches import ingest
from intake.rows import ObservationRow
from lake_cli import initialize_receiving, initialize_source
from lake_db import open_database
from publication.history import read
from publication.signing import sign
from receiving.intake import receive
from receiving.requests import register_assignment, register_request
from scan.handoff import begin_handoff
from scan.requests import request
from scan.segments import scan
from scan.tasks import assign_task, record_task_state


ROWS = (
    ObservationRow(40, "北闸/A", 7, "可用"),
    ObservationRow(12, "内务/B", 91, "待核"),
    ObservationRow(31, "南闸/C", 11, "可用"),
)
EXPECTED = (
    FrozenRow(0, 40, "北闸/A", 7, "可用"),
    FrozenRow(2, 31, "南闸/C", 11, "可用"),
)


def populate(source, primary, standby):
    """Load the example through the ordinary intake and replica APIs."""
    ingest(source, "F", ROWS)
    revision_id = freeze(source, "F")
    revision = get_revision(source, revision_id)
    for db, name, groups in (
        (primary, "primary", ((1, 0), (2,))),
        (standby, "standby", ((0,), (2, 1))),
    ):
        layout = ReplicaLayout.from_positions(revision, enumerate(groups))
        replicate(source, db, revision_id, name, layout)
    return revision_id


def main():
    with ExitStack() as stack:
        def opened(initialize):
            db = stack.enter_context(closing(open_database(":memory:")))
            initialize(db)
            return db

        source = opened(initialize_source)
        primary, standby = opened(initialize_replica_store), opened(initialize_replica_store)
        inbox = opened(initialize_receiving)
        revision_id = populate(source, primary, standby)
        obligation = request(source, revision_id, "fixture", 0, 3, "可用")
        first = assign_task(source, "fixture", "primary", "primary-task").assignment
        register_request(inbox, obligation, first)
        first_segment = scan(source, primary, first.task_id, 0)
        first_receipt = receive(inbox, "fixture", first_segment)
        record_task_state(source, first.task_id, "lost")
        backup = begin_handoff(source, "fixture", first.task_id, "standby", "standby-task").assignment
        register_assignment(inbox, backup)
        tail_segment = scan(source, standby, backup.task_id, 1)
        source.close()
        if first_segment.checked_positions != (0, 1) or tail_segment.checked_positions != (1, 2):
            raise RuntimeError("样例分组没有形成预期的逻辑交叠")
        receive(inbox, "fixture", tail_segment)
        if receive(inbox, "fixture", first_segment) != first_receipt:
            raise RuntimeError("未复领原实收回执")
        result = sign(inbox, "fixture")
        if (result.details, result.total_liters) != (EXPECTED, 18) or read(inbox, result.result_id) != result:
            raise RuntimeError("普通抽查的样例明细、升数或首执不符")
        print("V1 fixture F: positions 0,2; 18 L; original receipt retained")


if __name__ == "__main__":
    main()
