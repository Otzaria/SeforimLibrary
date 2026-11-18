"""Imported complete-notes recovery fails closed without modifying retained work."""
import json
import hashlib
from pathlib import Path
import tempfile
import unittest
import resume_complete_notes as rc
import vector_shard as vs

PACKAGE='5f80c47d3310a21fcc0c2f541fbb5732ba254555ef45afc2712cab63c8a33059'
TAG='v38-20261112163847'


def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value))


def fixture(work,model_path,warehouse,tag=TAG,window=2,count=5):
    model={'family_id':'otzaria/Meivin-Embed-QA491-native256','tokenizer_checksum':'a'*64,
           'embedding_dim':256,'pooling':'mean','max_tokens':256,'query_max_tokens':128,
           'embedding_text_version':4,'normalization_version':1,'chunking_identity':12470596585207933847,
           'query_packages':[{'checksum':PACKAGE,'quantization':'fp32'}]}
    write(model_path,model)
    write(work/'library-release.json',{'tag_name':tag,'published_at':'2026-11-12T16:38:47Z'})
    write(work/'dl/otzaria-library-index.provenance.json',{'schemaVersion':1,'libraryReleaseTag':tag})
    (work/'seforim.db').write_bytes(b'verified DB')
    source=work/'index/index';private=work/'complete-notes-index'
    for path in (source,private):
        write(path/'meta.json',{'segments':[{'segment_id':'f'*32}]})
        write(path/'otzaria_book_order.json',{'canonical':['book']})
    write(source/'otzaria_index_meta.json',{'schema_version':10})
    write(private/'otzaria_index_meta.json',{'schema_version':11,'semantic_projection_version':1})
    write(private/'build-provenance.json',{'format':'otzaria-semantic-index-rebuild','version':1,
          'library_version':int(tag.split('-')[0][1:]),'db_schema_version':7,
          'ids_text_and_metadata_verified':True,'source_documents':5,'rebuilt_documents':5,
          'database_sha256':vs.sha256_file(work/'seforim.db'),
          'source_index_meta_sha256':vs.sha256_file(source/'meta.json'),
          'source_book_order_sha256':vs.sha256_file(source/'otzaria_book_order.json'),
          'source_index_metadata':json.loads((source/'otzaria_index_meta.json').read_text()),
          'rebuilt_index_metadata':json.loads((private/'otzaria_index_meta.json').read_text())})
    fw=work/rc.FAMILY;plan=fw/'plan'
    embed=vs.write_embed_plan(plan,(str(i) for i in range(count)),model,model['query_packages'][0])
    for name in ['books.json','book-hashes.json','records.bin','tombstones.bin']:(plan/name).write_bytes(b'fixture')
    files={p.name:{'sha256':vs.sha256_file(p),'size':p.stat().st_size} for p in plan.iterdir()}
    write(plan/'plan-manifest.json',{'format':'otzaria-vector-plan','version':1,'identity':{'model':model},
          'library_release_tag':tag,'previous':None,'parity':{'mismatches':0},
          'counts':{'to_embed':count,'unique':count+3},'files':files})
    for n in range((count+window-1)//window):
        shard=fw/'shards'/f's{n:03d}'
        vs.write_shard(plan,shard,n*window,window,lambda texts:[[1.]+[0.]*255 for _ in texts],
          {'name':'seforim-gpu-worker','version':'1.1','device':'AMD Radeon RX 9060 XT','ep':'rocm','mode':'torch-mixed','model_kind':'meivin'},
          parity_fn=lambda _:({'samples':1000,'min_cosine':.9999},{'parity':'verified'}))
        write(shard/'token-audit.json',{'format':'otzaria-passage-token-audit','version':1,
              'plan_sha256':embed['plan_sha256'],'skip':n*window,'tokenizer_sha256':model['tokenizer_checksum'],
              'max_tokens':256,'passages':min(window,count-n*window),'over_budget_passages':1,'discarded_tokens':3,'longest_passage_tokens':259})
    identity={k:model[k] for k in ('family_id','tokenizer_checksum','embedding_dim','pooling','max_tokens')}
    identity['passage_package']=embed['passage_package']
    vectors_digest=hashlib.sha256();keys_digest=hashlib.sha256()
    for shard in sorted((fw/'shards').iterdir()):
        vectors_digest.update((shard/'vectors.f32').read_bytes());keys_digest.update((shard/'keys.bin').read_bytes())
    write(warehouse/'warehouse.json',{'format':'otzaria-vector-warehouse','version':1,'identity':identity,
          'records':count+3,'batches':[{'plan_sha256':embed['plan_sha256'],'records':count,'shards':(count+window-1)//window,'vectors_sha256':vectors_digest.hexdigest(),'keys_sha256':keys_digest.hexdigest()}]})
    (fw/'release').mkdir();(fw/'release/oversized.oxv').write_bytes(b'original oversized release')
    return fw


class Recovery(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.work=self.root/'work';self.model=self.root/'model.json';self.warehouse=self.root/'warehouse'
        self.fw=fixture(self.work,self.model,self.warehouse)
        self.cli=self.root/'cli';self.cli.write_text('#!/bin/sh\nexit 0\n');self.cli.chmod(0o755)
        self.args=(self.work,TAG,self.model,PACKAGE,2,self.warehouse,self.cli)

    def snapshot(self):
        return {str(p):(p.stat().st_mtime_ns,p.read_bytes()) for root in (self.work,self.warehouse) for p in root.rglob('*') if p.is_file()}

    def refused(self):
        before=self.snapshot()
        with self.assertRaises(vs.ContractError):rc.validate(*self.args)
        self.assertEqual(before,self.snapshot())

    def change(self,path,fn):
        m=json.loads(path.read_text());fn(m);write(path,m)

    def test_imported_partial_plan_preserves_bytes_and_mtimes(self):
        before=self.snapshot();result=rc.validate(*self.args);after=self.snapshot()
        self.assertEqual(result['verified_records'],5);self.assertEqual(result['verified_shards'],3)
        self.assertEqual(result['over_budget_passages'],3);self.assertEqual(result['discarded_tokens'],9)
        self.assertEqual(before,{k:v for k,v in after.items() if k in before});self.assertEqual(len(after)-len(before),1)
        self.assertTrue(result['full_index_and_retrieval_validation_required'])

    def test_corrupt_vector_refuses_without_mutation(self):
        p=self.fw/'shards/s000/vectors.f32';p.write_bytes(b'x'+p.read_bytes()[1:]);self.refused()

    def test_nonfinite_vector_with_updated_checksum_refuses(self):
        import struct
        p=self.fw/'shards/s000/vectors.f32';p.write_bytes(struct.pack('<f',float('nan'))+p.read_bytes()[4:])
        self.change(p.parent/'shard-manifest.json',lambda m:m.update(vectors_sha256=vs.sha256_file(p)));self.refused()

    def test_bad_parity_refuses(self):
        self.change(self.fw/'shards/s000/shard-manifest.json',lambda m:m['parity'].update(min_cosine=.998));self.refused()

    def test_bad_token_audit_counts_refuse(self):
        self.change(self.fw/'shards/s000/token-audit.json',lambda m:m.update(over_budget_passages=3));self.refused()

    def test_missing_window_refuses(self):
        (self.fw/'shards/s002/shard-manifest.json').unlink();self.refused()

    def test_wrong_model_refuses(self):
        self.change(self.model,lambda m:m.update(max_tokens=128));self.refused()

    def test_modified_book_hashes_refuses(self):
        (self.fw/'plan/book-hashes.json').write_bytes(b'changed');self.refused()

    def test_modified_database_refuses(self):
        (self.work/'seforim.db').write_bytes(b'changed');self.refused()

    def test_modified_source_index_refuses(self):
        write(self.work/'index/index/meta.json',{'segments':[]});self.refused()

    def test_modified_private_provenance_refuses(self):
        write(self.work/'complete-notes-index/otzaria_index_meta.json',{'semantic_projection_version':0});self.refused()

    def test_missing_import_batch_refuses(self):
        self.change(self.warehouse/'warehouse.json',lambda m:m.update(batches=[]));self.refused()

    def test_import_batch_must_match_actual_retained_vectors(self):
        self.change(self.warehouse/'warehouse.json',lambda m:m['batches'][0].update(vectors_sha256='b'*64));self.refused()

    def test_other_warehouse_identity_refuses(self):
        self.change(self.warehouse/'warehouse.json',lambda m:m['identity'].update(max_tokens=128));self.refused()

    def test_native_warehouse_verifier_refuses_without_repair(self):
        self.cli.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "'+str(self.root/'args')+'"\nexit 1\n')
        self.refused();self.assertNotIn('--repair',(self.root/'args').read_text())

    def test_corrupt_parity_document_refuses(self):
        (self.fw/'shards/s000/parity-certificate.json').write_bytes(b'changed');self.refused()

    def test_wrong_token_audit_refuses(self):
        self.change(self.fw/'shards/s000/token-audit.json',lambda m:m.update(skip=2));self.refused()

    def test_symlink_refuses(self):
        (self.fw/'extra').symlink_to(self.model);self.refused()


if __name__=='__main__':unittest.main()
