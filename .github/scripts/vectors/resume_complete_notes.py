"""Verify a completed, already-imported complete-notes build without inference.

Runs under the driver's global lock. Retained inputs are read-only; the only
output is a receipt after every check, including the warehouse's native verifier.
Coverage, text/book hashes, resolution and recall are subsequently revalidated
against the retained private index by validate_semantic_vectors.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import uuid

import vector_shard as vs
from resume_compact import read, require

FAMILY = 'meivin-qa491-complete-notes-onnx'
PLAN_FILES = {'books.json', 'book-hashes.json', 'embed-manifest.json', 'embed.jsonl', 'records.bin', 'tombstones.bin'}


def hashed(path, digest, size=None, aggregate=None, vectors=False):
    require(path.is_file() and not path.is_symlink(), f'missing regular retained file: {path}')
    require(size is None or path.stat().st_size == size, f'retained file size changed: {path}')
    if aggregate is None:
        actual = vs.sha256_file(path)
    else:
        h = hashlib.sha256()
        with path.open('rb') as f:
            for block in iter(lambda: f.read(1 << 24), b''):
                h.update(block); aggregate.update(block)
                if vectors:
                    import numpy as np
                    values = np.frombuffer(block, dtype='<f4').reshape(-1, 256)
                    require(np.isfinite(values).all(), f'nonfinite retained vectors: {path}')
                    norms = np.sqrt(np.square(values.astype(np.float64)).sum(axis=1))
                    require((np.abs(norms - 1.0) <= vs.UNIT_NORM_TOLERANCE).all(),
                            f'nonunit retained vectors: {path}')
        actual = h.hexdigest()
    require(actual == digest, f'retained file checksum changed: {path}')


def validate(work, tag, model_path, package, window, warehouse, cli):
    require(work.is_dir() and not work.is_symlink(), 'resume needs an existing work directory')
    require(not any(p.is_symlink() for p in work.rglob('*')), 'retained work contains a symlink')
    require(window > 0, 'window must be positive')
    version = int(tag.split('-')[0][1:])
    release = read(work / 'library-release.json')
    require(release.get('tag_name') == tag and release.get('published_at'), 'retained release differs')
    provenance = read(work / 'dl/otzaria-library-index.provenance.json')
    require(provenance.get('schemaVersion') == 1 and provenance.get('libraryReleaseTag') == tag,
            'retained source index provenance differs')
    source = work / 'index/index'
    index = work / 'complete-notes-index'
    rebuilt = read(index / 'build-provenance.json')
    require(rebuilt.get('format') == 'otzaria-semantic-index-rebuild' and rebuilt.get('version') == 1
            and rebuilt.get('library_version') == version and rebuilt.get('db_schema_version') == 7
            and rebuilt.get('ids_text_and_metadata_verified') is True
            and rebuilt.get('source_documents') == rebuilt.get('rebuilt_documents') > 0,
            'retained private index has no verified canonical rebuild')
    hashed(work / 'seforim.db', rebuilt['database_sha256'])
    hashed(source / 'meta.json', rebuilt['source_index_meta_sha256'])
    hashed(source / 'otzaria_book_order.json', rebuilt['source_book_order_sha256'])
    require(read(source / 'otzaria_index_meta.json') == rebuilt['source_index_metadata'], 'source index metadata changed')
    meta = read(index / 'otzaria_index_meta.json')
    require(meta == rebuilt['rebuilt_index_metadata'] and meta.get('semantic_projection_version') == 1,
            'private index semantic provenance changed')
    # Tantivy opens and checks the referenced segments during full G3/G4 validation.
    require(read(index / 'meta.json').get('segments'), 'private index has no committed segments')
    require((index / 'otzaria_book_order.json').read_bytes() == (source / 'otzaria_book_order.json').read_bytes(),
            'private index catalogue order changed')
    fw = work / FAMILY
    plan_dir = fw / 'plan'
    plan = read(plan_dir / 'plan-manifest.json')
    model = read(model_path)
    require(plan.get('format') == 'otzaria-vector-plan' and plan.get('version') == 1
            and plan.get('library_release_tag') == tag and plan.get('previous') is None
            and plan.get('parity', {}).get('mismatches') == 0, 'retained base plan differs or failed G2')
    require(plan.get('identity', {}).get('model') == model, 'candidate model identity changed')
    require(model.get('embedding_dim') == 256 and model.get('embedding_text_version') == 4
            and model.get('chunking_identity') == 12470596585207933847, 'not the complete-notes recipe')
    require(set(plan.get('files', {})) == PLAN_FILES, 'unexpected plan file set')
    for name, item in plan['files'].items():
        hashed(plan_dir / name, item['sha256'], item['size'])
    embed = vs.read_embed_manifest(plan_dir)
    require(embed['model'] == model and embed['passage_package'] == {'checksum': package, 'quantization': 'fp32'},
            'embedding identity or package changed')
    require(embed['plan_sha256'] == plan['files']['embed.jsonl']['sha256']
            and embed['records'] == plan['counts']['to_embed'] > 0, 'embedding plan digest/count changed')
    shards = fw / 'shards'
    expected = {f's{n:03d}' for n in range((embed['records'] + window - 1) // window)}
    require(shards.is_dir() and {p.name for p in shards.iterdir()} == expected, 'all completed shard windows are required')
    total = 0
    vectors_digest = hashlib.sha256(); keys_digest = hashlib.sha256()
    overflows = discarded = 0
    for n, shard in enumerate(sorted(shards.iterdir())):
        m = read(shard / vs.SHARD_MANIFEST_FILE)
        count = min(window, embed['records'] - n * window)
        require(m.get('format') == vs.SHARD_FORMAT and m.get('version') == 2
                and m.get('skip') == n * window and m.get('take') == window and m.get('records') == count
                and m.get('dim') == 256 and m.get('plan_sha256') == embed['plan_sha256']
                and m.get('model') == model and m.get('passage_package') == embed['passage_package'],
                f'completed shard contract changed: {shard.name}')
        hashed(shard / 'vectors.f32', m['vectors_sha256'], count * 256 * 4, vectors_digest, vectors=True)
        hashed(shard / 'keys.bin', m['keys_sha256'], count * 32, keys_digest)
        worker = m.get('worker', {})
        require(worker.get('name') == 'seforim-gpu-worker' and worker.get('version') == '1.1'
                and worker.get('ep') == 'rocm' and worker.get('mode') == 'torch-mixed'
                and worker.get('model_kind') == 'meivin', f'worker contract differs: {shard.name}')
        vs.check_parity(m['parity'])
        hashed(shard / vs.PARITY_DOCUMENT_FILE, m['parity']['document_sha256'])
        audit = read(shard / 'token-audit.json')
        require(audit.get('format') == 'otzaria-passage-token-audit' and audit.get('version') == 1
                and audit.get('plan_sha256') == embed['plan_sha256'] and audit.get('skip') == n * window
                and audit.get('tokenizer_sha256') == model['tokenizer_checksum']
                and audit.get('max_tokens') == model['max_tokens'] and audit.get('passages') == count,
                f'token audit contract differs: {shard.name}')
        require(all(type(audit.get(k)) is int and audit[k] >= 0 for k in
                    ('over_budget_passages', 'discarded_tokens', 'longest_passage_tokens'))
                and audit['over_budget_passages'] <= count
                and audit['discarded_tokens'] >= audit['over_budget_passages']
                and (audit['over_budget_passages'] == 0) == (audit['discarded_tokens'] == 0)
                and (audit['longest_passage_tokens'] > model['max_tokens']) == (audit['over_budget_passages'] > 0),
                f'token audit counts are inconsistent: {shard.name}')
        total += count
        overflows += audit['over_budget_passages']; discarded += audit['discarded_tokens']
    wm = read(warehouse / 'warehouse.json')
    identity = {k: model[k] for k in ('family_id', 'tokenizer_checksum', 'embedding_dim', 'pooling', 'max_tokens')}
    identity['passage_package'] = embed['passage_package']
    require(wm.get('format') == 'otzaria-vector-warehouse' and wm.get('version') == 1
            and wm.get('identity') == identity, 'warehouse identity changed')
    batches = [b for b in wm['batches'] if b.get('plan_sha256') == embed['plan_sha256']]
    require(len(batches) == 1 and batches[0]['records'] == total and batches[0]['shards'] == len(expected),
            'warehouse does not contain the complete imported plan batch')
    require(batches[0].get('vectors_sha256') == vectors_digest.hexdigest()
            and batches[0].get('keys_sha256') == keys_digest.hexdigest(),
            'warehouse batch differs from the retained completed shards')
    checked = subprocess.run([str(cli), 'warehouse-verify', '--warehouse', str(warehouse)], check=False,
                             text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    require(checked.returncode == 0, f'warehouse verification failed (no repair attempted): {checked.stdout}')
    print(checked.stdout, end='', file=sys.stderr)
    receipt = {'status': 'VERIFIED_COMPLETE_NOTES_RECOVERY', 'tag': tag,
               'plan_sha256': embed['plan_sha256'], 'plan_manifest_sha256': vs.sha256_file(plan_dir / 'plan-manifest.json'),
               'database_sha256': rebuilt['database_sha256'], 'verified_records': total,
               'verified_shards': len(expected), 'over_budget_passages': overflows, 'discarded_tokens': discarded,
               'warehouse_manifest_sha256': vs.sha256_file(warehouse / 'warehouse.json'),
               'rebuild_provenance_sha256': vs.sha256_file(index / 'build-provenance.json'),
               'full_index_and_retrieval_validation_required': True}
    vs.write_json_atomic(work / ('complete-notes-resume-' + uuid.uuid4().hex + '.json'), receipt)
    return receipt


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    for name in ('work', 'model', 'warehouse', 'cli'):
        ap.add_argument('--' + name, type=Path, required=True)
    for name in ('tag', 'package'):
        ap.add_argument('--' + name, required=True)
    ap.add_argument('--window', type=int, required=True)
    a = ap.parse_args()
    try:
        print(json.dumps(validate(a.work, a.tag, a.model, a.package, a.window, a.warehouse, a.cli)), flush=True)
    except (vs.ContractError, OSError, ValueError, KeyError, TypeError) as e:
        print(f'complete-notes resume refused (retained work unchanged): {e}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
