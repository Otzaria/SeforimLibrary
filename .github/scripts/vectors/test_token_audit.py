import json
from pathlib import Path
import random
import tempfile
import unittest

import reference
import token_audit
import vector_shard as vs

try:
    from tokenizers import Tokenizer, models, pre_tokenizers, processors
except ImportError:
    Tokenizer = None


@unittest.skipIf(Tokenizer is None, "tokenizers unavailable")
class TokenAudit(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.path = self.root / "tokenizer.json"
        self.tok = Tokenizer(models.WordLevel({"[UNK]": 0, "[CLS]": 1, "[SEP]": 2,
            "[PASSAGE]": 3, "alpha": 4, "beta": 5, "שלום": 6}, unk_token="[UNK]"))
        self.tok.pre_tokenizer = pre_tokenizers.Whitespace()
        self.tok.add_special_tokens(["[CLS]", "[SEP]", "[PASSAGE]"])
        self.tok.post_processor = processors.TemplateProcessing(single="[CLS] $A [SEP]",
            special_tokens=[("[CLS]", 1), ("[SEP]", 2)])
        self.tok.save(str(self.path))
        self.cap = reference.load_tokenizer(str(self.path), 8)
        self.plan = {"plan_sha256": "a" * 64, "model": {
            "max_tokens": 8, "tokenizer_checksum": vs.sha256_file(self.path)}}

    def tearDown(self):
        self.temp.cleanup()

    def test_complete_counts_and_exact_production_ids_for_random_and_special_inputs(self):
        rng = random.Random(34)
        texts = ["[PASSAGE] " + " ".join(rng.choices(
            ["alpha", "beta", "שלום", "🙂", "[CLS]", "[SEP]"], k=rng.randrange(1, 80)))
            for _ in range(150)]
        audit = token_audit.PassageAudit(self.path, self.cap, self.plan, self.root, 7)
        actual = audit.encode(texts)
        expected = [e.ids for e in self.cap.encode_batch(texts, add_special_tokens=True)]
        self.assertEqual(actual, expected)
        lengths = [len(self.tok.encode(text, add_special_tokens=True).ids) for text in texts]
        report = audit.finish(len(texts))
        self.assertEqual(report["passages"], len(texts))
        self.assertEqual(report["over_budget_passages"], sum(n > 8 for n in lengths))
        self.assertEqual(report["discarded_tokens"], sum(max(0, n - 8) for n in lengths))
        self.assertEqual(report["longest_passage_tokens"], max(lengths))
        self.assertEqual(report["examples"][0]["record"], 7)
        self.assertEqual(json.loads((self.root / "token-audit.json").read_text()), report)
        self.assertFalse((self.root / "token-audit.partial.json").exists())

    def test_incomplete_count_cannot_be_announced_as_complete(self):
        audit = token_audit.PassageAudit(self.path, self.cap, self.plan, self.root, 0)
        audit.encode(["[PASSAGE] alpha"])
        with self.assertRaises(vs.ContractError):
            audit.finish(2)
        self.assertFalse((self.root / "token-audit.json").exists())
        self.assertTrue((self.root / "token-audit.partial.json").exists())

    def test_wrong_tokenizer_is_rejected_before_counting(self):
        self.plan["model"]["tokenizer_checksum"] = "f" * 64
        with self.assertRaisesRegex(vs.ContractError, "tokenizer checksum"):
            token_audit.PassageAudit(self.path, self.cap, self.plan, self.root, 0)


if __name__ == "__main__":
    unittest.main()
