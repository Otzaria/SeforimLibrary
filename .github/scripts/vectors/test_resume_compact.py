"""Recovery must never erase/recompute committed work or trust a corrupt shard."""
import json
from pathlib import Path
import tempfile
import unittest

import resume_compact as rc
import vector_shard as vs


def fixture(work, model_path, tag='v37-20261105163520', window=2, count=5, package='b'*64):
    work.mkdir(parents=True, exist_ok=True)
    fw = work / rc.FAMILY
    plan = fw / 'plan'
    model = {'family_id': 'otzaria/Pardes-QA491-int8-384', 'tokenizer_checksum': 'a'*64,
             'embedding_dim': 384, 'pooling': 'in-graph', 'max_tokens': 512, 'query_max_tokens': 128,
             'embedding_text_version': 3, 'normalization_version': 2, 'chunking_identity': 13334152532150206612,
             'query_packages': [{'checksum': package, 'quantization': 'int8'}]}
    model_path.write_text(json.dumps(model))
    vs.write_embed_plan(plan, (str(i) for i in range(count)), model, model['query_packages'][0])
    for name in ['books.json', 'book-hashes.json', 'records.bin', 'tombstones.bin']:
        (plan/name).write_bytes(b'fixture')
    files = {p.name: {'sha256': vs.sha256_file(p), 'size': p.stat().st_size} for p in plan.iterdir()}
    (plan/'plan-manifest.json').write_text(json.dumps({'format': 'otzaria-vector-plan', 'version': 1,
        'identity': {'model': model}, 'library_release_tag': tag, 'previous': None,
        'parity': {'mismatches': 0}, 'counts': {'to_embed': count, 'unique': count}, 'files': files}))
    (work/'library-release.json').write_text(json.dumps({'tag_name': tag, 'published_at': '2026-11-05T23:58:39Z'}))
    (work/'dl').mkdir(exist_ok=True)
    (work/'dl/otzaria-library-index.provenance.json').write_text(json.dumps({'libraryReleaseTag': tag}))
    (work/'index/index').mkdir(parents=True, exist_ok=True)
    (work/'index/index/segment').write_bytes(b'index')
    (work/'seforim.db').write_bytes(b'db')
    worker={'name': 'seforim-onnx-cpu-worker', 'device': 'cpu', 'runtime': '1.28.0',
            'graph_sha256': rc.GRAPH_SHA, 'one_text_per_call': True, 'ep': 'cpu', 'mode': 'onnxruntime'}
    def add(n):
        out = fw/'shards'/f's{n:03d}'
        vs.write_shard(plan, out, n*window, window, lambda texts: [[1.]+[0.]*383 for _ in texts], worker)
        return out
    return add


class Resume(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name); self.work=self.root/'work'; self.model=self.root/'model.json'
        self.add=fixture(self.work,self.model); self.shard=self.add(0)
        self.partial=self.work/rc.FAMILY/'shards/s001'
        self.partial.mkdir(); (self.partial/'vectors.f32.partial').write_bytes(b'uncommitted')
        self.args=(self.work,'v37-20261105163520',self.model,'b'*64,2,self.root/'warehouse')

    def snapshot(self):
        return {str(p.relative_to(self.work)):(p.stat().st_mtime_ns,p.read_bytes()) for p in self.work.rglob('*') if p.is_file()}

    def refused_unchanged(self):
        before=self.snapshot()
        with self.assertRaises(rc.vs.ContractError): rc.validate(*self.args)
        self.assertEqual(self.snapshot(),before)

    def test_completed_bytes_and_mtimes_untouched_partial_archived_intact(self):
        before={p.name:(p.read_bytes(),p.stat().st_mtime_ns) for p in self.shard.iterdir()}
        receipt=rc.validate(*self.args)
        self.assertEqual(receipt['verified_records'],2)
        self.assertEqual({p.name:(p.read_bytes(),p.stat().st_mtime_ns) for p in self.shard.iterdir()},before)
        self.assertEqual((Path(receipt['partial_directories_preserved_at'][0])/'vectors.f32.partial').read_bytes(),b'uncommitted')
        self.assertFalse(self.partial.exists())

    def test_corrupt_complete_shard_refuses_before_archiving_partial(self):
        p=self.shard/'vectors.f32'; b=p.read_bytes();p.write_bytes(b'x'+b[1:]);self.refused_unchanged()

    def test_wrong_model_refuses(self):
        m=json.loads(self.model.read_text());m['query_max_tokens']=256;self.model.write_text(json.dumps(m));self.refused_unchanged()

    def test_modified_plan_refuses(self):
        with (self.work/rc.FAMILY/'plan/embed.jsonl').open('ab') as f:f.write(b' ')
        self.refused_unchanged()

    def test_other_release_refuses(self):
        p=self.work/'library-release.json';m=json.loads(p.read_text());m['tag_name']='v38-20261105163520';p.write_text(json.dumps(m));self.refused_unchanged()

    def test_wrong_window_position_refuses(self):
        p=self.shard/'shard-manifest.json';m=json.loads(p.read_text());m['skip']=1;p.write_text(json.dumps(m));self.refused_unchanged()

    def test_partial_final_window_is_a_valid_complete_shard(self):
        self.add(2);r=rc.validate(*self.args);self.assertEqual(r['verified_records'],3)

    def test_existing_warehouse_cannot_be_silently_recreated(self):
        p=self.root/'warehouse';p.mkdir();(p/'warehouse.json').write_text('{}');self.refused_unchanged()

    def test_completed_manifest_with_missing_file_is_not_discarded_as_partial(self):
        (self.shard/'keys.bin').unlink();self.refused_unchanged()

    def test_linked_shard_is_refused_without_mutation(self):
        (self.partial/'link').symlink_to(self.model);self.refused_unchanged()

if __name__=='__main__':unittest.main()
