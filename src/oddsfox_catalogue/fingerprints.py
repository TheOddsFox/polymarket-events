"""Streaming schema and semantic-row digests; physical Parquet bytes are separate."""

import hashlib
import json

SEMANTIC_DIGEST_REVISION = "sha256-row-multiset-v1"


def semantic_fingerprint(connection, query, parameters=None):
    connection.execute("SET TimeZone = 'UTC'")
    parameters = parameters or []
    description = connection.execute(
        f"SELECT * FROM ({query}) AS r LIMIT 0", parameters
    ).description
    schema = [{"name": name, "type": str(kind)} for name, kind, *_ in description]
    digest = hashlib.sha256(json.dumps(schema, sort_keys=True).encode() + b"\n")
    cursor = connection.execute(
        f"SELECT sha256(to_json(r)) AS row FROM ({query}) AS r ORDER BY row", parameters
    )
    count = 0
    while result := cursor.fetchone():
        digest.update(result[0].encode() + b"\n")
        count += 1
    return {
        "rows": count,
        "schema": schema,
        "semantic_sha256": digest.hexdigest(),
        "semantic_digest_revision": SEMANTIC_DIGEST_REVISION,
    }
