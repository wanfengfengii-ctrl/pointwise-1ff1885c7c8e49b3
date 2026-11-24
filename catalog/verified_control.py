"""Preparation material kept in the independent control database.

After the source database is shut down, dispatching and scanning use only this
control connection plus prepared replica databases.  Nothing here copies frozen
row text: only batch/revision identity, the logical length, non-disclosing
commitment salts, prepared group membership and the issued credential bytes.
"""

import json
from dataclasses import dataclass

from lake_db import canonical_json, initialize_schema, write_transaction


@dataclass(frozen=True, slots=True)
class PreparedBatch:
    revision_id: str
    batch_id: str
    length: int
    credential: bytes


def initialize_prepared(db):
    initialize_schema(db, '''
        CREATE TABLE IF NOT EXISTS prepared_batches (
            revision_id TEXT PRIMARY KEY,
            batch_id TEXT NOT NULL,
            length INTEGER NOT NULL,
            credential BLOB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS prepared_salts (
            revision_id TEXT NOT NULL,
            position INTEGER NOT NULL,
            value_salt BLOB NOT NULL,
            origin_salt BLOB NOT NULL,
            quality_salt BLOB NOT NULL,
            original_salt BLOB NOT NULL,
            PRIMARY KEY (revision_id, position)
        );
        CREATE TABLE IF NOT EXISTS prepared_groups (
            revision_id TEXT NOT NULL,
            replica_id TEXT NOT NULL,
            group_number INTEGER NOT NULL,
            positions TEXT NOT NULL,
            PRIMARY KEY (revision_id, replica_id, group_number)
        );
    ''')


def initialize_control(db):
    """Create the control-side schema needed for offline dispatch and scanning."""
    from catalog.replicas import initialize_replica_catalog
    from catalog.revisions import initialize_revisions
    from intake.batches import initialize_batch_store
    from scan.requests import initialize_scan_requests
    from scan.tasks import initialize_scan_tasks

    initialize_batch_store(db)
    initialize_revisions(db)
    initialize_replica_catalog(db)
    initialize_scan_requests(db)
    initialize_scan_tasks(db)
    initialize_prepared(db)


def load_prepared(db, revision_id):
    row = db.execute('''
        SELECT batch_id, length, credential FROM prepared_batches WHERE revision_id = ?
    ''', (revision_id,)).fetchone()
    return None if row is None else PreparedBatch(revision_id, row[0], row[1], bytes(row[2]))


def prepared_replica_ids(db, revision_id):
    rows = db.execute('''
        SELECT DISTINCT replica_id FROM prepared_groups WHERE revision_id = ?
    ''', (revision_id,))
    return frozenset(row[0] for row in rows)


def prepared_group_positions(db, revision_id, replica_id, group_number):
    row = db.execute('''
        SELECT positions FROM prepared_groups
        WHERE revision_id = ? AND replica_id = ? AND group_number = ?
    ''', (revision_id, replica_id, group_number)).fetchone()
    return None if row is None else tuple(json.loads(row[0]))


def load_salts(db, revision_id):
    salts = {}
    for position, value, origin, quality, original in db.execute('''
        SELECT position, value_salt, origin_salt, quality_salt, original_salt
        FROM prepared_salts WHERE revision_id = ?
    ''', (revision_id,)):
        salts[position] = {'value': bytes(value), 'origin': bytes(origin),
                           'quality': bytes(quality), 'original': bytes(original)}
    return salts


def store_prepared(db, prepared, replica_manifests, salts, groups):
    """Write headers, replica manifests and non-disclosing material atomically."""
    with write_transaction(db):
        db.execute('INSERT OR IGNORE INTO batches VALUES (?)', (prepared.batch_id,))
        header = db.execute('SELECT batch_id FROM frozen_revisions WHERE revision_id = ?',
                            (prepared.revision_id,)).fetchone()
        if header is None:
            db.execute('INSERT INTO frozen_revisions VALUES (?, ?)',
                       (prepared.revision_id, prepared.batch_id))
        elif header[0] != prepared.batch_id:
            raise ValueError('revision is already prepared for a different batch')
        for replica_id, manifest in replica_manifests:
            body = canonical_json([[number, digest] for number, digest in manifest])
            existing = db.execute('''
                SELECT revision_id, manifest FROM available_replicas WHERE replica_id = ?
            ''', (replica_id,)).fetchone()
            if existing is None:
                db.execute('INSERT INTO available_replicas VALUES (?, ?, ?)',
                           (replica_id, prepared.revision_id, body))
            elif tuple(existing) != (prepared.revision_id, body):
                raise ValueError('replica is already registered differently')
        db.execute('INSERT INTO prepared_batches VALUES (?, ?, ?, ?)',
                   (prepared.revision_id, prepared.batch_id, prepared.length, prepared.credential))
        db.executemany('INSERT INTO prepared_salts VALUES (?, ?, ?, ?, ?, ?)', (
            (prepared.revision_id, position, salt['value'], salt['origin'],
             salt['quality'], salt['original'])
            for position, salt in sorted(salts.items())
        ))
        db.executemany('INSERT INTO prepared_groups VALUES (?, ?, ?, ?)', (
            (prepared.revision_id, replica_id, number, canonical_json(list(positions)))
            for (replica_id, number), positions in sorted(groups.items())
        ))


def store_replica_material(replica_db, replica_id, revision_id, groups):
    """Plant the offline grouping record inside an already-loaded replica."""
    initialize_schema(replica_db, '''
        CREATE TABLE IF NOT EXISTS verified_prepared_groups (
            replica_id TEXT NOT NULL,
            revision_id TEXT NOT NULL,
            group_number INTEGER NOT NULL,
            positions TEXT NOT NULL,
            PRIMARY KEY (replica_id, revision_id, group_number)
        );
    ''')
    with write_transaction(replica_db):
        replica_db.executemany('''
            INSERT OR REPLACE INTO verified_prepared_groups VALUES (?, ?, ?, ?)
        ''', ((replica_id, revision_id, number, canonical_json(list(positions)))
              for (candidate, number), positions in sorted(groups.items())
              if candidate == replica_id))
