"""Contract checks for the isolated Pardes profile and model implementation."""
import unittest
from pathlib import Path
import hashlib,subprocess
HERE=Path(__file__).resolve().parent
class PardesProfile(unittest.TestCase):
 def test_new_profile_resets_to_old_defaults(self):
  script='source pins.env; source model_profiles.sh; select_model_profile pardes107-windows-onnx; echo "$MODEL_WORKER_KIND $MODEL_GRAPH $QUERY_GRAPH $MODEL_TOKEN_BUDGET $MODEL_MAX_BATCH"; select_model_profile meivin-qa491-onnx; echo "$MODEL_WORKER_KIND $MODEL_GRAPH $MODEL_TOKEN_BUDGET $MODEL_MAX_BATCH"'
  out=subprocess.check_output(['bash','-c','HERE="$PWD"; '+script],cwd=HERE,text=True).splitlines()
  self.assertEqual(out,['pardes107 model.onnx model.onnx 8192 32','meivin document.fp32.onnx 32768 1024'])
 def test_pardes_not_added_to_automatic_families(self):
  out=subprocess.check_output(['bash','-c','source pins.env; echo "$FAMILY_CONFIGS"; echo "$OPT_IN_FAMILY_CONFIGS"'],cwd=HERE,text=True).splitlines()
  self.assertNotIn('pardes107',out[0]);self.assertIn('pardes107',out[1])
if __name__=='__main__':unittest.main()
