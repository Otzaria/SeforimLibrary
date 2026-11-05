#!/usr/bin/env python3
"""Compare every schema object and table value (including IDs/BLOBs) in two DBs.

Usage: python tool/perf/generator_compare.py baseline.db candidate.db
Only SQLite's internal objects are excluded. Output is JSON; differences exit nonzero.
"""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import sqlite3


def quoted(name):
    return '"' + name.replace('"', '""') + '"'


def snapshot(path):
    result = {}
    with sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True) as db:
        result['user_version'] = db.execute('PRAGMA user_version').fetchone()[0]
        result['application_id'] = db.execute('PRAGMA application_id').fetchone()[0]
        result['integrity'] = db.execute('PRAGMA integrity_check').fetchall()
        result['foreign_keys'] = db.execute('PRAGMA foreign_key_check').fetchall()
        schema = db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name").fetchall()
        result['schema_sha256'] = hashlib.sha256(json.dumps(schema, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()
        tables = {}
        for (table,) in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"):
            columns = [row[1] for row in db.execute('PRAGMA table_info(' + quoted(table) + ')')]
            order = ','.join(quoted(column) for column in columns)
            digest = hashlib.sha256()
            count = 0
            for row in db.execute('SELECT * FROM ' + quoted(table) + ' ORDER BY ' + order):
                encoded = [dict(blob=base64.b64encode(value).decode('ascii')) if isinstance(value, bytes) else value for value in row]
                data = json.dumps(encoded, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode()
                digest.update(len(data).to_bytes(8, 'big'))
                digest.update(data)
                count += 1
            tables[table] = {'rows': count, 'sha256': digest.hexdigest()}
        result['tables'] = tables
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('baseline')
    parser.add_argument('candidate')
    args = parser.parse_args()
    baseline, candidate = snapshot(args.baseline), snapshot(args.candidate)
    equal = baseline == candidate
    result = {'equal': equal, 'baseline': baseline, 'candidate': candidate}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if equal and baseline['integrity'] == [('ok',)] and not baseline['foreign_keys'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
