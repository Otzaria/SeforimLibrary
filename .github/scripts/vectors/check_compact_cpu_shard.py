"""Actual compact graph -> atomic shard -> independent per-text reference roundtrip."""
import argparse, hashlib, json, subprocess, sys
from pathlib import Path

import numpy as np
import reference,vector_shard,model_package


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--model-dir',required=True,type=Path)
    ap.add_argument('--out',required=True,type=Path)
    args=ap.parse_args()
    assert not args.out.exists()
    args.out.mkdir(parents=True)
    model=json.loads((args.model_dir/'model.json').read_text())
    package={'checksum':model_package.package_checksum(args.model_dir,'model.onnx'),'quantization':'int8'}
    texts=['שלום עולם','query: מהו דין מוקצה בשבת?','בְּרֵאשִׁית בָּרָא אֱלֹהִים','a',
           'תלמוד בבלי '+'אמר רב '*500,'punctuation / אב״ג _ brackets []',
           'לימוד אמונה והלכה במקורות התלמוד ובפירושי הראשונים']
    texts += ['טקסט מספר '+str(i) for i in range(10)]
    plan=args.out/'plan';shard=args.out/'shard'
    vector_shard.write_embed_plan(plan,texts,model,package)
    command=[sys.executable,str(Path(__file__).with_name('embed_worker.py')),'--plan',str(plan),'--out',str(shard),
             '--model-dir',str(args.model_dir),'--graph','model.onnx','--model-kind','pardesqa491',
             '--device','cpu','--cpu-procs','2','--cpu-threads','1','--chunk','8']
    run=subprocess.run(command,capture_output=True,text=True,timeout=90)
    if run.returncode:raise RuntimeError(run.stderr[-3000:])
    manifest=json.loads((shard/'shard-manifest.json').read_text())
    got=np.fromfile(shard/'vectors.f32',dtype='<f4').reshape(len(texts),384)
    tok=reference.load_tokenizer(str(args.model_dir/'tokenizer.json'),512)
    ids=[e.ids for e in tok.encode_batch(texts)]
    expected=reference.embed_reference(str(args.model_dir/'model.onnx'),ids,procs=1)
    assert manifest['worker']['mode']=='onnxruntime' and manifest['worker']['ep']=='cpu'
    assert manifest['passage_package']==package and 'parity' not in manifest
    assert (shard/'keys.bin').read_bytes()==b''.join(hashlib.sha256(t.encode()).digest() for t in texts)
    assert np.array_equal(got,expected),float(np.abs(got-expected).max())
    old=(shard/'vectors.f32').read_bytes()
    rerun=subprocess.run(command,capture_output=True,text=True,timeout=20)
    assert rerun.returncode!=0 and 'finished shard' in rerun.stderr
    assert (shard/'vectors.f32').read_bytes()==old
    report={'status':'PASS_ACTUAL_EXACT_COMPACT_CPU_SHARD','rows':len(texts),'max_tokens':max(map(len,ids)),
            'max_abs':float(np.abs(got-expected).max()),'cosine_min':float(reference.cosines(got,expected).min()),
            'order_and_keys_verified':True,'immutable_completed_shard_verified':True,'manifest':manifest,
            'worker_stdout':run.stdout,'GPU_calls':0,'training_updates':0,'TEST_calls':0}
    (args.out/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({k:v for k,v in report.items() if k not in ['manifest','worker_stdout']}))

if __name__=='__main__':main()
