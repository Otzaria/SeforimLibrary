"""Contract checks for the isolated Pardes profile and model implementation."""
import unittest
from pathlib import Path
import hashlib,subprocess,types,sys
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
import embed_worker
class PardesProfile(unittest.TestCase):
 def test_new_profile_resets_to_old_defaults(self):
  script='source pins.env; source model_profiles.sh; select_model_profile pardesqa491-windows-onnx; echo "$MODEL_WORKER_KIND $MODEL_GRAPH $QUERY_GRAPH $MODEL_TOKEN_BUDGET $MODEL_MAX_BATCH $MODEL_DEVICE"; select_model_profile meivin-qa491-onnx; echo "$MODEL_WORKER_KIND $MODEL_GRAPH $MODEL_TOKEN_BUDGET $MODEL_MAX_BATCH $MODEL_DEVICE"'
  out=subprocess.check_output(['bash','-c','HERE="$PWD"; '+script],cwd=HERE,text=True).splitlines()
  self.assertEqual(out,['pardesqa491 model.onnx model.onnx 8192 32 cpu','meivin document.fp32.onnx 32768 1024 cuda'])
 def test_pardes_not_added_to_automatic_families(self):
  out=subprocess.check_output(['bash','-c','source pins.env; echo "$FAMILY_CONFIGS"; echo "$OPT_IN_FAMILY_CONFIGS"'],cwd=HERE,text=True).splitlines()
  self.assertNotIn('pardesqa491',out[0]);self.assertIn('pardesqa491',out[1])
  self.assertNotIn('pardes107',' '.join(out))
 def test_compact_cannot_silently_use_cuda_or_fp32_package(self):
  plan={'model':{'family_id':'otzaria/Pardes-QA491-int8-384'},'passage_package':{'quantization':'int8'}}
  with self.assertRaisesRegex(embed_worker.vector_shard.ContractError,'GPU emulation is not qualified'):
   embed_worker.compact_onnx_cpu(types.SimpleNamespace(device='cuda'),plan,1,0)
  plan['passage_package']['quantization']='fp32'
  with self.assertRaisesRegex(embed_worker.vector_shard.ContractError,'isolated INT8 family'):
   embed_worker.compact_onnx_cpu(types.SimpleNamespace(device='cpu'),plan,1,0)
 def test_cpu_parameters_require_positive_counts(self):
  for flag in ['--cpu-procs','--cpu-threads']:
   with self.assertRaises(SystemExit):embed_worker.parse_args(['--plan','p','--out','o',flag,'0'])
if __name__=='__main__':unittest.main()
