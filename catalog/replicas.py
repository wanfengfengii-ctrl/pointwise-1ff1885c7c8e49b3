import hashlib
import json
import secrets
from dataclasses import dataclass

from catalog.layouts import LayoutGroup, validate_layout
from catalog.revisions import decode_frozen_row, encode_frozen_row, get_revision
from catalog.verified import (SALT_LENGTH, BatchCredential, CredentialGroup, CredentialReplica,
                              commit_row, encode_credential)
from catalog.verified_control import (PreparedBatch, initialize_control, load_prepared,
                                      store_prepared, store_replica_material)
from intake.validation import require_text
from lake_db import canonical_json, initialize_schema, write_transaction


@dataclass(frozen=True, slots=True)
class Replica:
    replica_id: str
    revision_id: str
    manifest: tuple[tuple[int, str], ...]

    @property
    def group_numbers(self):
        return tuple(number for number, checksum in self.manifest)


def initialize_replica_catalog(db):
    initialize_schema(db, '''
        CREATE TABLE IF NOT EXISTS available_replicas (
            replica_id TEXT PRIMARY KEY,
            revision_id TEXT NOT NULL REFERENCES frozen_revisions(revision_id),
            manifest TEXT NOT NULL
        );
    ''')


def initialize_replica_store(db):
    initialize_schema(db, '''
        CREATE TABLE IF NOT EXISTS replica_identity (
            replica_id TEXT PRIMARY KEY,
            revision_id TEXT NOT NULL,
            manifest TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS replica_groups (
            replica_id TEXT NOT NULL REFERENCES replica_identity(replica_id),
            group_number INTEGER NOT NULL,
            body TEXT NOT NULL,
            PRIMARY KEY (replica_id, group_number)
        );
    ''')


def _digest(body):
    return hashlib.sha256(body.encode('utf-8')).hexdigest()


def get_replica(db, revision_id, replica_id):
    record = db.execute('''
        SELECT manifest FROM available_replicas WHERE replica_id = ? AND revision_id = ?
    ''', (replica_id, revision_id)).fetchone()
    if record is None:
        raise LookupError(f'replica {replica_id!r} unavailable for {revision_id!r}')
    return Replica(replica_id, revision_id, tuple((number, digest) for number, digest in json.loads(record[0])))


def replicate(db, replica_db, revision_id, replica_id, layout):
    require_text(replica_id, 'replica ID')
    if replica_db is db:
        raise ValueError('use an independent replica database')
    revision = get_revision(db, revision_id)
    validate_layout(revision, layout)
    groups = tuple((group.number, canonical_json([encode_frozen_row(row) for row in group.rows]))
                   for group in layout.groups)
    manifest = tuple((number, _digest(body)) for number, body in groups)
    header = (revision_id, canonical_json(manifest))
    existing = db.execute('SELECT revision_id, manifest FROM available_replicas WHERE replica_id = ?',
                          (replica_id,)).fetchone()
    if existing is not None and existing != header:
        raise ValueError('replica ID already has a different layout or revision')

    with write_transaction(replica_db):
        local = replica_db.execute('SELECT revision_id, manifest FROM replica_identity WHERE replica_id = ?',
                                   (replica_id,)).fetchone()
        if local is None:
            replica_db.execute('INSERT INTO replica_identity VALUES (?, ?, ?)', (replica_id, *header))
            replica_db.executemany('INSERT INTO replica_groups VALUES (?, ?, ?)',
                                   ((replica_id, number, body) for number, body in groups))
        else:
            if local != header:
                raise ValueError('replica store already has a different layout or revision')
            stored = replica_db.execute('SELECT group_number, body FROM replica_groups WHERE replica_id = ?',
                                        (replica_id,))
            if dict(stored) != dict(groups):
                raise ValueError('existing replica group contents differ')

    # A committed local copy can be retried if registering it in the catalog fails.
    with write_transaction(db):
        current = db.execute('SELECT revision_id, manifest FROM available_replicas WHERE replica_id = ?',
                             (replica_id,)).fetchone()
        if current is None:
            db.execute('INSERT INTO available_replicas VALUES (?, ?, ?)', (replica_id, *header))
        elif current != header:
            raise ValueError('conflicting replica registration')
    return Replica(replica_id, revision_id, manifest)


def read_replica_group(replica_db, replica, number):
    if type(number) is not int:
        raise ValueError('group number must be an integer')
    checksum = dict(replica.manifest).get(number)
    if checksum is None:
        raise LookupError(f'group {number} is absent from replica {replica.replica_id!r}')
    record = replica_db.execute('''
        SELECT g.body FROM replica_groups AS g JOIN replica_identity AS i USING (replica_id)
        WHERE g.replica_id = ? AND i.revision_id = ? AND g.group_number = ?
    ''', (replica.replica_id, replica.revision_id, number)).fetchone()
    if record is None or _digest(record[0]) != checksum:
        raise ValueError(f'unverified replica data for group {number}')
    return LayoutGroup(number, tuple(map(decode_frozen_row, json.loads(record[0]))))


def prepare(source, dispatch, revision_id, replicas):
    """Open a frozen batch for verified sampling using an independent control store.

    Returns the trusted batch credential bytes.  Salts, prepared manifests and
    the issued credential are written to ``dispatch`` and to each replica so
    dispatch and scanning no longer need ``source`` after shutdown.
    """
    require_text(revision_id, 'revision ID')
    if dispatch is source:
        raise ValueError('use an independent control database')
    if not isinstance(replicas, (list, tuple)) or not replicas:
        raise ValueError('prepare requires at least one prepared replica')
    revision = get_revision(source, revision_id)
    if _has_prepared(dispatch):
        existing = load_prepared(dispatch, revision_id)
        if existing is not None:
            raise ValueError('frozen batch is already opened for verified sampling')

    prepared_replicas = []
    manifests = []
    groups = {}
    salts = {}
    replica_handles = []
    seen_ids = set()
    for entry in replicas:
        if not isinstance(entry, (list, tuple)) or len(entry) != 2:
            raise ValueError('expected (replica_id, replica_db) pairs')
        replica_id, replica_db = entry
        require_text(replica_id, 'replica ID')
        if replica_id in seen_ids:
            raise ValueError('replica ID listed twice')
        seen_ids.add(replica_id)
        if replica_db is source or replica_db is dispatch:
            raise ValueError('prepared replicas must be independent stores')
        replica = get_replica(source, revision_id, replica_id)
        if any(replica_db is handle[1] for handle in replica_handles):
            raise ValueError('the same replica store is listed twice')
        members = {}
        for number in replica.group_numbers:
            group = read_replica_group(replica_db, replica, number)
            positions = tuple(row.position for row in group.rows)
            groups[(replica_id, number)] = positions
            for position, row in zip(positions, group.rows):
                if row != revision.rows[position]:
                    raise ValueError(f'replica {replica_id!r} differs at frozen position {position}')
                members.setdefault(position, row)
        if sorted(members) != list(range(len(revision.rows))):
            raise ValueError(f'replica {replica_id!r} does not cover the frozen batch')
        prepared_replicas.append(CredentialReplica(replica_id, tuple(
            CredentialGroup(number, groups[(replica_id, number)])
            for number in sorted(replica.group_numbers))))
        manifests.append((replica_id, replica.manifest))
        replica_handles.append((replica_id, replica_db, tuple(sorted(replica.group_numbers))))
        for position, row in members.items():
            salts.setdefault(position, row)

    if sorted(salts) != list(range(len(revision.rows))):
        raise ValueError('prepared replicas do not cover the frozen batch')

    salt_rows = {}
    credentials = []
    for position in range(len(revision.rows)):
        row = salts[position]
        row_salts = {name: secrets.token_bytes(SALT_LENGTH)
                     for name in ('value', 'origin', 'quality', 'original')}
        salt_rows[position] = row_salts
        credentials.append(commit_row(revision_id, position, row, row_salts))

    credential = BatchCredential(revision_id, revision.batch_id, len(revision.rows),
                                 tuple(credentials), tuple(prepared_replicas))
    data = encode_credential(credential)
    prepared = PreparedBatch(revision_id, revision.batch_id, len(revision.rows), data)

    initialize_control(dispatch)
    # Replica-side grouping is idempotent; the control commit is the last step.
    for replica_id, replica_db, _ in replica_handles:
        store_replica_material(replica_db, replica_id, revision_id, groups)
    store_prepared(dispatch, prepared, manifests, salt_rows, groups)
    return data


def _has_prepared(db):
    return db.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'prepared_batches'"
                      ).fetchone() is not None
