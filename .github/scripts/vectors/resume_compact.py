"""Validate an interrupted compact base build, retaining every completed shard.

Called while the driver holds STATE/.lock. Only the embedding phase before the
first warehouse import is resumable. Any later-phase recovery needs reconciliation.
Uncommitted directories are moved intact outside shards, after all checks succeed.
"""
from __future__ import annotations

import argparse
import datetime
import json
import math
import os
from pathlib import Path
import re
import sys
import uuid

import vector_shard as vs

FAMILY = 'pardesqa491-windows-onnx'
GRAPH_SHA = 'c0a9ca9d764ae40ebba313150305702fd82832312ae9e11a7eb449e5a388f090'


def require(condition, message):
    if not condition:
        raise vs.ContractError(message)


def read(path):
    require(path.is_file() and not path.is_symlink(), f'not a regular retained file: {path}')
    return json.loads(path.read_text())


def validate(work: Path, tag: str, model: Path, package: str, window: int, warehouse: Path):
    require(work.is_dir() and not work.is_symlink(), 'resume needs an existing work directory')
    # Refuse symlink aliases before reading or moving anything from retained work.
    require(not any(p.is_symlink() for p in work.rglob('*')), 'retained work contains a symlink')
    require(window > 0, 'window must be positive')
    require(not warehouse.exists() or not any(warehouse.iterdir()),
            'resume is limited to embedding before warehouse import; retain work for reconciliation')
    fw = work / FAMILY
    require(not any((fw / n).exists() for n in ['release', 'delta', 'ledger']),
            'later build phase exists; retain work for reconciliation')
    release = read(work / 'library-release.json')
    require(release.get('tag_name') == tag and release.get('published_at'), 'retained release differs')
    provenance = read(work / 'dl/otzaria-library-index.provenance.json')
    require(provenance.get('libraryReleaseTag') == tag, 'retained index is for another release')
    require((work / 'index/index').is_dir() and any((work / 'index/index').iterdir()), 'retained index is missing')
    require((work / 'seforim.db').is_file() and (work / 'seforim.db').stat().st_size > 0, 'retained DB missing')
    plan_dir = fw / 'plan'
    plan = read(plan_dir / 'plan-manifest.json')
    require(plan.get('format') == 'otzaria-vector-plan' and plan.get('version') == 1, 'invalid plan version')
    require(plan.get('library_release_tag') == tag and plan.get('previous') is None, 'plan is not this base')
    require(plan.get('parity', {}).get('mismatches') == 0, 'plan key parity failed')
    require(plan.get('identity', {}).get('model') == read(model), 'model identity changed')
    require(set(plan.get('files', {})) == {'books.json', 'book-hashes.json', 'embed-manifest.json', 'embed.jsonl', 'records.bin', 'tombstones.bin'}, 'unexpected plan file set')
    for name, expected in plan['files'].items():
        path = plan_dir / name
        require(path.is_file() and path.stat().st_size == expected['size'] and
                vs.sha256_file(path) == expected['sha256'], f'plan file changed: {name}')
    embed = vs.read_embed_manifest(plan_dir)
    require(embed['model'] == plan['identity']['model'], 'embed model differs from plan')
    require(embed['passage_package'] == {'checksum': package, 'quantization': 'int8'}, 'package changed')
    require(embed['plan_sha256'] == plan['files']['embed.jsonl']['sha256'], 'embed plan digest differs')
    require(embed['records'] == plan['counts']['to_embed'] == plan['counts']['unique'] > 0, 'plan is not a fresh full base')
    shards = fw / 'shards'
    require(shards.is_dir(), 'retained shards missing')
    windows = math.ceil(embed['records'] / window)
    complete, unfinished = [], []
    for shard in sorted(shards.iterdir()):
        require(shard.is_dir() and re.fullmatch(r's[0-9]{3,}', shard.name), f'unexpected shard path: {shard}')
        n = int(shard.name[1:]); skip = n * window
        require(n < windows and shard.name == f's{n:03d}', 'shard lies outside plan or aliases a window')
        manifest = shard / vs.SHARD_MANIFEST_FILE
        if not manifest.exists():
            require(all(p.name in {'vectors.f32', 'keys.bin', 'vectors.f32.partial', 'keys.bin.partial', 'shard-manifest.json.partial'} and p.is_file() for p in shard.iterdir()), 'unexpected unfinished shard content')
            unfinished.append(shard)
            continue
        m = read(manifest)
        records = min(window, embed['records'] - skip)
        require(m.get('format') == vs.SHARD_FORMAT and m.get('version') == 2 and
                m.get('skip') == skip and m.get('take') == window and m.get('records') == records and
                m.get('dim') == 384 and m.get('plan_sha256') == embed['plan_sha256'] and
                m.get('model') == embed['model'] and m.get('passage_package') == embed['passage_package'],
                f'completed shard contract differs: {shard.name}')
        worker = m.get('worker', {})
        require(worker.get('name') == 'seforim-onnx-cpu-worker' and worker.get('device') == 'cpu' and
                worker.get('runtime') == '1.28.0' and worker.get('graph_sha256') == GRAPH_SHA and
                worker.get('one_text_per_call') is True, f'worker policy changed: {shard.name}')
        require({p.name for p in shard.iterdir()} == {'vectors.f32', 'keys.bin', 'shard-manifest.json'}, f'unexpected complete shard content: {shard.name}')
        for filename, digest_key, size in [('vectors.f32', 'vectors_sha256', records * 384 * 4), ('keys.bin', 'keys_sha256', records * 32)]:
            f = shard / filename
            require(f.is_file() and f.stat().st_size == size and vs.sha256_file(f) == m.get(digest_key),
                    f'completed shard corrupted: {shard.name}/{filename}')
        complete.append((shard, records))
    # All validation precedes any mutation. Keep incomplete bytes intact, away from warehouse-add.
    archived = []
    if unfinished:
        dest = work / 'recovery-partials' / (datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8])
        dest.mkdir(parents=True)
        for p in unfinished:
            target = dest / p.name
            p.rename(target); archived.append(str(target))
    receipt = {'status': 'VERIFIED_COMPACT_RESUME', 'tag': tag, 'plan_sha256': embed['plan_sha256'],
               'verified_shards': len(complete), 'verified_records': sum(n for _, n in complete),
               'total_records': embed['records'], 'kept_shards': [p.name for p, _ in complete],
               'partial_directories_preserved_at': archived}
    receipt_path = work / ('resume-verification-' + uuid.uuid4().hex + '.json')
    with receipt_path.open('x') as f:
        json.dump(receipt, f, indent=2); f.write('\n'); f.flush(); os.fsync(f.fileno())
    return receipt


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    for name in ('work', 'model', 'warehouse'):
        ap.add_argument('--' + name, type=Path, required=True)
    for name in ('tag', 'package'):
        ap.add_argument('--' + name, required=True)
    ap.add_argument('--window', type=int, required=True)
    args = ap.parse_args()
    try:
        print(json.dumps(validate(args.work, args.tag, args.model, args.package, args.window, args.warehouse)), flush=True)
    except (vs.ContractError, OSError, ValueError, KeyError, TypeError) as e:
        print(f'resume refused (completed work unchanged): {e}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
