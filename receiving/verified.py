"""Isolation-side records for verified sampling.

The batch credential handed over before shutdown is persisted here, so request
mode and credential binding survive reopening the receiving database.  Each
accepted verified segment keeps its disclosure proof next to the ordinary
receipt so a replay can be checked without the source database.
"""

import sqlite3

from lake_db import initialize_schema


def initialize_verified_receipts(db):
    initialize_schema(db, '''
        CREATE TABLE IF NOT EXISTS receiving_verified_requests (
            request_id TEXT PRIMARY KEY REFERENCES receiving_requests(request_id),
            credential BLOB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS verified_acceptances (
            receipt_id INTEGER PRIMARY KEY REFERENCES accepted_segments(receipt_id),
            request_id TEXT NOT NULL,
            task_id TEXT NOT NULL,
            group_number INTEGER NOT NULL,
            proof BLOB NOT NULL,
            UNIQUE (request_id, task_id, group_number)
        );
    ''')


def _table_exists(db, name):
    return db.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                      (name,)).fetchone() is not None


def is_verified(db, request_id):
    if not _table_exists(db, 'receiving_verified_requests'):
        return False
    return db.execute('SELECT 1 FROM receiving_verified_requests WHERE request_id = ?',
                      (request_id,)).fetchone() is not None


def get_credential(db, request_id):
    row = db.execute('SELECT credential FROM receiving_verified_requests WHERE request_id = ?',
                     (request_id,)).fetchone()
    if row is None:
        raise LookupError(f'request {request_id!r} has no verified batch binding')
    return bytes(row[0])


def bind_credential(db, request_id, credential):
    try:
        db.execute('INSERT INTO receiving_verified_requests VALUES (?, ?)', (request_id, credential))
    except sqlite3.IntegrityError as error:
        raise ValueError('request already has a verified batch binding') from error


def store_acceptance(db, receipt_id, request_id, task_id, group_number, proof):
    db.execute('INSERT INTO verified_acceptances VALUES (?, ?, ?, ?, ?)',
               (receipt_id, request_id, task_id, group_number, proof))


def find_acceptance_proof(db, request_id, task_id, group_number):
    row = db.execute('''
        SELECT proof FROM verified_acceptances
        WHERE request_id = ? AND task_id = ? AND group_number = ?
    ''', (request_id, task_id, group_number)).fetchone()
    return None if row is None else bytes(row[0])


def verified_acceptance_exists(db, request_id, task_id, group_number):
    return find_acceptance_proof(db, request_id, task_id, group_number) is not None
