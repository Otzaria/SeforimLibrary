"""Count complete passage sequences while producing the unchanged capped token IDs.

The tokenizer runs once, without postprocessing/truncation. The production
tokenizer then applies its own truncation and postprocessor to that Encoding.
This preserves exact inference IDs while exposing content otherwise discarded.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import vector_shard as vs


class PassageAudit:
    def __init__(self, tokenizer_path, capped, plan, out, skip):
        from tokenizers import Tokenizer
        tokenizer_path = Path(tokenizer_path)
        digest = vs.sha256_file(tokenizer_path)
        if digest != plan["model"]["tokenizer_checksum"]:
            raise vs.ContractError("token audit tokenizer checksum differs from the plan")
        self.uncapped = Tokenizer.from_file(str(tokenizer_path))
        self.uncapped.no_padding()
        self.uncapped.no_truncation()
        self.uncapped.encode_special_tokens = False
        self.capped = capped
        self.specials = capped.num_special_tokens_to_add(False)
        self.out = Path(out)
        self.skip = skip
        self.report = {
            "format": "otzaria-passage-token-audit", "version": 1,
            "plan_sha256": plan["plan_sha256"], "tokenizer_sha256": digest,
            "max_tokens": plan["model"]["max_tokens"], "skip": skip,
            "passages": 0, "over_budget_passages": 0, "discarded_tokens": 0,
            "longest_passage_tokens": 0, "examples": [],
            "policy": "count and report overflow; inference retains the declared right token cap",
        }

    def encode(self, texts):
        encoded = self.uncapped.encode_batch(texts, add_special_tokens=False)
        ids = []
        for text, encoding in zip(texts, encoded):
            tokens = len(encoding.ids) + self.specials
            record = self.skip + self.report["passages"]
            self.report["passages"] += 1
            self.report["longest_passage_tokens"] = max(self.report["longest_passage_tokens"], tokens)
            if tokens > self.report["max_tokens"]:
                self.report["over_budget_passages"] += 1
                self.report["discarded_tokens"] += tokens - self.report["max_tokens"]
                if len(self.report["examples"]) < 16:
                    self.report["examples"].append({"record": record, "tokens": tokens,
                        "key": hashlib.sha256(text.encode()).hexdigest()[:32]})
            ids.append(self.capped.post_process(encoding, add_special_tokens=True).ids)
        vs.write_json_atomic(self.out / "token-audit.partial.json", self.report)
        return ids

    def finish(self, records):
        if self.report["passages"] != records:
            raise vs.ContractError("token audit did not count every completed shard record")
        vs.write_json_atomic(self.out / "token-audit.json", self.report)
        (self.out / "token-audit.partial.json").unlink(missing_ok=True)
        return self.report
