"""Offline verification material for a frozen batch.

The credential is an opaque ``bytes`` value handed to the isolation side before
the source database is shut down.  It carries the batch identity, the logical
length, the prepared replica grouping (logical positions only) and one salted
commitment per frozen position and field.  Salts are never part of the
credential, so the isolation side cannot test a candidate value against a
commitment for a field that was not disclosed to it.

A verified segment carries ``proof`` bytes: for every position the segment
claims to have checked it opens either the full frozen row (a hit) or only the
quality field (a non-hit).  The isolation side recomputes the commitments and
therefore independently decides the actual checked range and every hit
determination.
"""

import hashlib
import json
import re
from dataclasses import dataclass

from catalog.revisions import FrozenRow
from intake.rows import decode_liters, encode_liters
from lake_db import canonical_json


CREDENTIAL_FORMAT = 'qy-verified-batch/1'
PROOF_FORMAT = 'qy-verified-segment/1'
SALT_LENGTH = 32

_HEX32 = re.compile(r'^[0-9a-f]{64}$')
_HEX = re.compile(r'^[0-9a-f]+$')


class CredentialError(ValueError):
    pass


class ProofError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class PositionCommitment:
    position: int
    value: str
    origin: str
    quality: str
    original: str

    def field(self, name):
        return getattr(self, name)


@dataclass(frozen=True, slots=True)
class CredentialGroup:
    number: int
    positions: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class CredentialReplica:
    replica_id: str
    groups: tuple[CredentialGroup, ...]

    def group(self, number):
        for candidate in self.groups:
            if candidate.number == number:
                return candidate
        return None


@dataclass(frozen=True, slots=True)
class BatchCredential:
    revision_id: str
    batch_id: str
    length: int
    commitments: tuple[PositionCommitment, ...]
    replicas: tuple[CredentialReplica, ...]

    def commitment(self, position):
        if type(position) is not int or not 0 <= position < self.length:
            return None
        return self.commitments[position]

    def replica(self, replica_id):
        for candidate in self.replicas:
            if candidate.replica_id == replica_id:
                return candidate
        return None


def field_commitment(revision_id, position, field, salt, value):
    """Salted commitment; withholding ``salt`` hides ``value`` from testing."""
    if type(salt) is not bytes or len(salt) != SALT_LENGTH:
        raise ValueError('commitment salt must be 32 bytes')
    payload = canonical_json([revision_id, position, field, salt.hex(), value])
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def commit_row(revision_id, position, row, salts):
    """Return the four field commitments for one frozen row."""
    return PositionCommitment(
        position,
        field_commitment(revision_id, position, 'value', salts['value'], encode_liters(row.liters)),
        field_commitment(revision_id, position, 'origin', salts['origin'], row.origin),
        field_commitment(revision_id, position, 'quality', salts['quality'], row.quality),
        field_commitment(revision_id, position, 'original', salts['original'],
                         format(row.original_position, 'x')),
    )


def encode_credential(credential):
    payload = {
        'format': CREDENTIAL_FORMAT,
        'revision_id': credential.revision_id,
        'batch_id': credential.batch_id,
        'length': credential.length,
        'commitments': [
            {'position': item.position, 'value': item.value, 'origin': item.origin,
             'quality': item.quality, 'original': item.original}
            for item in credential.commitments
        ],
        'replicas': [
            {'replica_id': replica.replica_id,
             'groups': [{'number': group.number, 'positions': list(group.positions)}
                        for group in replica.groups]}
            for replica in credential.replicas
        ],
    }
    return canonical_json(payload).encode('utf-8')


def _require_digest(value, what):
    if not isinstance(value, str) or not _HEX32.match(value):
        raise CredentialError(f'invalid {what} commitment')


def _positions(values, length, what):
    if not isinstance(values, list):
        raise CredentialError(f'invalid {what} positions')
    result = []
    for value in values:
        if type(value) is not int or not 0 <= value < length:
            raise CredentialError(f'{what} position outside the frozen batch')
        result.append(value)
    if len(set(result)) != len(result):
        raise CredentialError(f'{what} repeats a frozen position')
    return tuple(result)


def parse_credential(data):
    if type(data) is not bytes:
        raise CredentialError('batch credential must be bytes')
    try:
        payload = json.loads(data.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CredentialError('malformed batch credential') from error
    if not isinstance(payload, dict):
        raise CredentialError('malformed batch credential')
    if set(payload) != {'format', 'revision_id', 'batch_id', 'length', 'commitments', 'replicas'}:
        raise CredentialError('unexpected batch credential fields')
    if payload['format'] != CREDENTIAL_FORMAT:
        raise CredentialError('unsupported batch credential format')
    revision_id = payload['revision_id']
    batch_id = payload['batch_id']
    if not isinstance(revision_id, str) or not revision_id or not isinstance(batch_id, str) or not batch_id:
        raise CredentialError('invalid batch identity')
    length = payload['length']
    if type(length) is not int or length <= 0:
        raise CredentialError('invalid frozen length')

    commitments = payload['commitments']
    if not isinstance(commitments, list) or len(commitments) != length:
        raise CredentialError('credential does not commit every frozen position')
    parsed = []
    for expected, item in enumerate(commitments):
        if not isinstance(item, dict) or set(item) != {'position', 'value', 'origin', 'quality', 'original'}:
            raise CredentialError('invalid position commitment')
        if item['position'] != expected:
            raise CredentialError('commitments are not in frozen order')
        for name in ('value', 'origin', 'quality', 'original'):
            _require_digest(item[name], name)
        parsed.append(PositionCommitment(expected, item['value'], item['origin'],
                                         item['quality'], item['original']))

    replicas = payload['replicas']
    if not isinstance(replicas, list) or not replicas:
        raise CredentialError('credential has no prepared replica')
    parsed_replicas = []
    seen_replicas = set()
    for item in replicas:
        if not isinstance(item, dict) or set(item) != {'replica_id', 'groups'}:
            raise CredentialError('invalid prepared replica')
        replica_id = item['replica_id']
        if not isinstance(replica_id, str) or not replica_id or replica_id in seen_replicas:
            raise CredentialError('invalid or repeated replica identity')
        seen_replicas.add(replica_id)
        groups = item['groups']
        if not isinstance(groups, list) or not groups:
            raise CredentialError('prepared replica has no groups')
        parsed_groups = []
        numbers = set()
        covered = []
        for group in groups:
            if not isinstance(group, dict) or set(group) != {'number', 'positions'}:
                raise CredentialError('invalid prepared group')
            number = group['number']
            if type(number) is not int or number < 0 or number in numbers:
                raise CredentialError('invalid or repeated replica group number')
            numbers.add(number)
            positions = _positions(group['positions'], length, 'group')
            if not positions:
                raise CredentialError('prepared group is empty')
            covered.extend(positions)
            parsed_groups.append(CredentialGroup(number, positions))
        if sorted(covered) != list(range(length)):
            raise CredentialError('prepared replica does not cover the frozen batch exactly once')
        parsed_replicas.append(CredentialReplica(replica_id, tuple(parsed_groups)))

    return BatchCredential(revision_id, batch_id, length, tuple(parsed), tuple(parsed_replicas))


def encode_proof(revision_id, request, segment, rows, salts):
    """Build the disclosure proof for one verified segment.

    Only positions the segment reports as checked get an opening; any row
    outside the reported range is dropped so a proof can never leak an
    out-of-window read value, origin or quality fact.
    """
    hits = {row.position: row for row in segment.hits}
    checked = set(segment.checked_positions)
    openings = []
    for row in rows:
        if row.position not in checked:
            continue
        row_salts = salts[row.position]
        if row.position in hits:
            openings.append({
                'position': row.position,
                'hit': True,
                'value_salt': row_salts['value'].hex(),
                'origin_salt': row_salts['origin'].hex(),
                'quality_salt': row_salts['quality'].hex(),
                'original_salt': row_salts['original'].hex(),
                'liters': encode_liters(row.liters),
                'origin': row.origin,
                'quality': row.quality,
                'original_position': format(row.original_position, 'x'),
            })
        else:
            openings.append({
                'position': row.position,
                'hit': False,
                'quality_salt': row_salts['quality'].hex(),
                'quality': row.quality,
            })
    payload = {
        'format': PROOF_FORMAT,
        'request_id': segment.request_id,
        'revision_id': segment.revision_id,
        'task_id': segment.task_id,
        'replica_id': segment.replica_id,
        'group_number': segment.group_number,
        'checked_positions': list(segment.checked_positions),
        'openings': openings,
    }
    return canonical_json(payload).encode('utf-8')


def _salt(entry, name):
    value = entry.get(name)
    if not isinstance(value, str) or not _HEX.match(value):
        raise ProofError(f'invalid {name}')
    try:
        raw = bytes.fromhex(value)
    except ValueError as error:
        raise ProofError(f'invalid {name}') from error
    if len(raw) != SALT_LENGTH:
        raise ProofError(f'invalid {name}')
    return raw


def _expect_keys(entry, keys, what):
    if not isinstance(entry, dict) or set(entry) != set(keys):
        raise ProofError(f'{what} carries missing or extra facts')


def _verify_hit(credential, position, commitment, entry, quality):
    _expect_keys(entry, ('position', 'hit', 'value_salt', 'origin_salt', 'quality_salt',
                         'original_salt', 'liters', 'origin', 'quality', 'original_position'),
                 'hit opening')
    value_salt = _salt(entry, 'value_salt')
    origin_salt = _salt(entry, 'origin_salt')
    quality_salt = _salt(entry, 'quality_salt')
    original_salt = _salt(entry, 'original_salt')

    liters = entry['liters']
    try:
        decoded_liters = decode_liters(liters)
    except (TypeError, ValueError) as error:
        raise ProofError('invalid hit read value') from error
    if decoded_liters is not None and type(decoded_liters) is not int:
        raise ProofError('invalid hit read value')
    origin = entry['origin']
    flag = entry['quality']
    if not isinstance(origin, str) or not origin:
        raise ProofError('invalid hit origin')
    if flag != quality:
        raise ProofError(f'position {position} does not carry the registered quality flag')
    original = entry['original_position']
    if not isinstance(original, str) or not _HEX.match(original):
        raise ProofError('invalid hit original position')

    revision_id = credential.revision_id
    checks = (
        ('value', value_salt, encode_liters(decoded_liters), commitment.value),
        ('origin', origin_salt, origin, commitment.origin),
        ('quality', quality_salt, flag, commitment.quality),
        ('original', original_salt, format(int(original, 16), 'x'), commitment.original),
    )
    for field, salt, value, expected in checks:
        if field_commitment(revision_id, position, field, salt, value) != expected:
            raise ProofError(f'hit fact at frozen position {position} does not match the credential')
    return FrozenRow(position, int(original, 16), origin, decoded_liters, flag)


def _verify_non_hit(credential, position, commitment, entry, quality):
    _expect_keys(entry, ('position', 'hit', 'quality_salt', 'quality'), 'non-hit opening')
    salt = _salt(entry, 'quality_salt')
    flag = entry['quality']
    if not isinstance(flag, str):
        raise ProofError('invalid non-hit quality flag')
    if field_commitment(credential.revision_id, position, 'quality', salt, flag) != commitment.quality:
        raise ProofError(f'non-hit fact at frozen position {position} does not match the credential')
    if flag == quality:
        raise ProofError(f'frozen position {position} is a hit but was reported unvisited')


def verify_proof(credential, request, segment, proof):
    """Independently validate a verified segment against the batch credential."""
    if type(proof) is not bytes:
        raise ProofError('verified segment is missing its proof')
    try:
        payload = json.loads(proof.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProofError('malformed verified segment proof') from error
    _expect_keys(payload, ('format', 'request_id', 'revision_id', 'task_id', 'replica_id',
                           'group_number', 'checked_positions', 'openings'), 'proof')
    if payload['format'] != PROOF_FORMAT:
        raise ProofError('unsupported verified segment proof format')
    if (payload['request_id'], payload['revision_id'], payload['task_id'],
            payload['replica_id'], payload['group_number']) != (
            segment.request_id, segment.revision_id, segment.task_id,
            segment.replica_id, segment.group_number):
        raise ProofError('proof belongs to a different segment')
    if payload['revision_id'] != credential.revision_id:
        raise ProofError('proof belongs to a different frozen batch')

    replica = credential.replica(segment.replica_id)
    if replica is None:
        raise ProofError('segment replica was not prepared for this batch')
    group = replica.group(segment.group_number)
    if group is None:
        raise ProofError('segment group was not prepared for this batch')

    expected = tuple(sorted(position for position in group.positions
                            if request.start <= position < request.stop))
    if not expected:
        raise ProofError('segment has no prepared position in the request window')
    raw_checked = payload['checked_positions']
    if not isinstance(raw_checked, list) or any(type(position) is not int for position in raw_checked):
        raise ProofError('invalid claimed checked positions')
    if tuple(raw_checked) != segment.checked_positions:
        raise ProofError('claimed checked positions differ from the segment')
    if tuple(raw_checked) != expected:
        raise ProofError('reported inspected range is not the prepared group intersection')

    checked = set(expected)
    hits = {}
    previous = -1
    for row in segment.hits:
        if not isinstance(row, FrozenRow) or type(row.position) is not int:
            raise ProofError('invalid hit row')
        if row.position not in checked or row.position <= previous:
            raise ProofError('hit rows must be ordered and within the checked positions')
        hits[row.position] = row
        previous = row.position
    if any(row.quality != request.quality for row in hits.values()):
        raise ProofError('hit row does not carry the registered quality flag')

    openings = payload['openings']
    if not isinstance(openings, list) or len(openings) != len(expected):
        raise ProofError('proof does not account for every checked position')
    seen = set()
    for entry in openings:
        if not isinstance(entry, dict) or type(entry.get('position')) is not int:
            raise ProofError('invalid opening')
        position = entry['position']
        if position in seen or position not in checked:
            raise ProofError('opening repeats or falls outside the checked positions')
        seen.add(position)
        commitment = credential.commitment(position)
        if commitment is None:
            raise ProofError('opening falls outside the frozen batch')
        if entry.get('hit') is True:
            row = _verify_hit(credential, position, commitment, entry, request.quality)
            if hits.get(position) != row:
                raise ProofError(f'reported hit at frozen position {position} differs from the frozen row')
        elif entry.get('hit') is False:
            if position in hits:
                raise ProofError(f'segment reports a hit at frozen position {position} '
                                 f'that the credential does not confirm')
            _verify_non_hit(credential, position, commitment, entry, request.quality)
        else:
            raise ProofError('opening has no valid hit determination')
    if seen != checked:
        raise ProofError('proof does not account for the reported inspected range')
    for position in hits:
        if position not in seen:
            raise ProofError(f'hit at frozen position {position} is not opened')
