"""CPU tests for the vector worker: the v2 shard contract, the package checksum and fetch,
and (when PyTorch is installed) the re-implementation's batching.

No GPU, no model and no network beyond a local HTTP server. Run:
    python3 .github/scripts/vectors/test_vectors_worker.py
"""

import contextlib
import hashlib
import http.server
import io
import json
import math
import os
import struct
import sys
import tempfile
import threading
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import model_package as mp  # noqa: E402
import vector_shard as vs  # noqa: E402

try:
    import numpy as np
    import torch
except ImportError:  # the CI image has neither; those tests skip
    np = torch = None

DIM = 4


def family(dim=DIM, fp32="f" * 64):
    return {"family_id": "test/family@0", "tokenizer_checksum": "0" * 64, "embedding_dim": dim,
            "pooling": "in-graph", "max_tokens": 256, "embedding_text_version": 2, "normalization_version": 1,
            "chunking_identity": 7, "query_packages": [{"checksum": "e" * 64, "quantization": "int8"},
                                                       {"checksum": fp32, "quantization": "fp32"}]}


def fake_vector(text, dim=DIM):
    h = hashlib.sha256(text.encode("utf-8")).digest()
    v = [((h[i] / 255.0) - 0.5) + 0.01 for i in range(dim)]
    n = math.sqrt(sum(x * x for x in v))
    return [x / n for x in v]


def fake_embed(texts):
    return [fake_vector(t) for t in texts]


WORKER = {"name": "seforim-gpu-worker", "version": "test", "device": "none", "ep": "cuda", "mode": "torch-mixed"}


def good_parity(plan):
    return {"reference": "test", "samples": 1000, "min_cosine": 0.9999, "mean_cosine": 0.99995}, {"doc": 1}


TEXTS = ["[PASSAGE] " + w for w in ("alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
                                     "iota", "kappa", "שלום עולם")]


class Plan(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.plan = self.root / "plan"
        fam = family()
        self.manifest = vs.write_embed_plan(self.plan, TEXTS, fam, fam["query_packages"][1])

    def tearDown(self):
        self.tmp.cleanup()


class ManifestAndRecords(Plan):
    def test_the_plan_is_read_back_and_its_digest_is_the_bytes_of_embed_jsonl(self):
        m = vs.read_embed_manifest(self.plan)
        self.assertEqual(m["records"], len(TEXTS))
        self.assertEqual(m["plan_sha256"], vs.sha256_file(self.plan / vs.EMBED_PLAN_FILE))
        line = json.loads((self.plan / vs.EMBED_PLAN_FILE).read_text(encoding="utf-8").splitlines()[-1])
        self.assertEqual(list(line), ["key", "embedding_text_sha256", "embedding_text"])
        self.assertEqual(line["key"], line["embedding_text_sha256"][:32])

    def test_a_manifest_of_another_version_or_package_is_refused(self):
        m = json.loads((self.plan / vs.EMBED_MANIFEST_FILE).read_text())
        for change in ({"version": 1}, {"passage_package": {"checksum": "a" * 64, "quantization": "fp32"}}):
            bad = dict(m, **change)
            (self.plan / vs.EMBED_MANIFEST_FILE).write_text(json.dumps(bad))
            with self.assertRaises(vs.ContractError):
                vs.read_embed_manifest(self.plan)

    def test_a_record_whose_text_or_key_does_not_match_its_digest_stops_the_worker(self):
        rec = json.loads((self.plan / vs.EMBED_PLAN_FILE).read_text(encoding="utf-8").splitlines()[0])
        vs.check_record(0, rec)
        with self.assertRaisesRegex(vs.ContractError, "damaged"):
            vs.check_record(0, dict(rec, embedding_text=rec["embedding_text"] + " "))
        with self.assertRaisesRegex(vs.ContractError, "key"):
            vs.check_record(0, dict(rec, key="0" * 32))

    def test_a_damaged_line_in_the_window_is_refused_and_outside_it_is_not_read(self):
        lines = (self.plan / vs.EMBED_PLAN_FILE).read_text(encoding="utf-8").splitlines(True)
        rec = json.loads(lines[5]); rec["embedding_text"] += "!"
        lines[5] = json.dumps(rec, ensure_ascii=False) + "\n"
        (self.plan / vs.EMBED_PLAN_FILE).write_text("".join(lines), encoding="utf-8")
        self.assertEqual(len(list(vs.read_window(self.plan, 0, 5))), 5)
        with self.assertRaises(vs.ContractError):
            list(vs.read_window(self.plan, 0, 6))

    def test_parity_samples_spread_over_the_whole_plan_and_repeat_for_every_shard(self):
        a = vs.sample_positions("ab" * 32, 1_000_000, 1000)
        self.assertEqual(a, vs.sample_positions("ab" * 32, 1_000_000, 1000))
        self.assertEqual(len(set(a)), 1000)
        self.assertTrue(all(i * 1000 <= p < (i + 1) * 1000 for i, p in enumerate(a)))
        self.assertEqual(len(vs.sample_positions("ab" * 32, 7, 1000)), 7)
        got = vs.read_positions(self.plan, [9, 2, 2])
        self.assertEqual([r.position for r in got], [2, 9])
        self.assertEqual(got[1].text, TEXTS[9])


class Shards(Plan):
    def shard(self, out, skip, take, embed=fake_embed, parity=good_parity, worker=WORKER, chunk=3):
        return vs.write_shard(self.plan, out, skip, take, embed, worker, parity_fn=parity, chunk=chunk)

    def test_two_windows_tile_the_plan_with_the_plans_keys_in_order(self):
        m1 = self.shard(self.root / "s1", 0, 6)
        m2 = self.shard(self.root / "s2", 6, 100)
        self.assertEqual((m1["records"], m2["records"], m2["take"]), (6, 5, 100))
        keys = (self.root / "s1" / vs.KEYS_FILE).read_bytes() + (self.root / "s2" / vs.KEYS_FILE).read_bytes()
        self.assertEqual(keys, b"".join(hashlib.sha256(t.encode()).digest() for t in TEXTS))
        for d, m in ((self.root / "s1", m1), (self.root / "s2", m2)):
            self.assertEqual(os.path.getsize(d / vs.VECTORS_FILE), m["records"] * DIM * 4)
            self.assertEqual(vs.sha256_file(d / vs.VECTORS_FILE), m["vectors_sha256"])
            self.assertEqual(vs.sha256_file(d / vs.KEYS_FILE), m["keys_sha256"])
            self.assertEqual(json.loads((d / vs.SHARD_MANIFEST_FILE).read_text()), m)
            self.assertEqual(m["format"], "otzaria-embed-shard"); self.assertEqual(m["version"], 2)
            self.assertEqual(m["plan_sha256"], self.manifest["plan_sha256"])
            self.assertEqual(m["model"], self.manifest["model"])
            self.assertEqual(m["passage_package"], self.manifest["passage_package"])
            self.assertEqual(m["parity"]["document_sha256"], vs.sha256_file(d / vs.PARITY_DOCUMENT_FILE))
            self.assertFalse(list(d.glob("*.partial")))
        first = struct.unpack("<4f", (self.root / "s1" / vs.VECTORS_FILE).read_bytes()[:16])
        self.assertTrue(all(abs(a - b) < 1e-6 for a, b in zip(first, fake_vector(TEXTS[0]))))

    def test_a_finished_shard_is_refused_and_never_overwritten(self):
        self.shard(self.root / "s", 0, 4)
        before = (self.root / "s" / vs.VECTORS_FILE).read_bytes()
        with self.assertRaisesRegex(vs.ContractError, "finished shard"):
            self.shard(self.root / "s", 0, 4)
        self.assertEqual((self.root / "s" / vs.VECTORS_FILE).read_bytes(), before)

    def test_a_session_that_dies_leaves_partial_files_and_no_manifest_and_a_retry_finishes(self):
        calls = []

        def dies(texts):
            calls.append(len(texts))
            if len(calls) == 2:
                raise RuntimeError("the GPU went away")
            return fake_embed(texts)
        with self.assertRaises(RuntimeError):
            self.shard(self.root / "s", 0, 9, embed=dies)
        d = self.root / "s"
        self.assertFalse((d / vs.SHARD_MANIFEST_FILE).exists())
        self.assertFalse((d / vs.VECTORS_FILE).exists())
        self.assertTrue((d / (vs.VECTORS_FILE + ".partial")).exists())
        m = self.shard(d, 0, 9)
        self.assertEqual(m["records"], 9)

    def test_the_manifest_is_written_last(self):
        seen = {}

        def parity(plan):
            seen["data_finished"] = (self.root / "s" / vs.VECTORS_FILE).exists() and (self.root / "s" / vs.KEYS_FILE).exists()
            seen["manifest"] = (self.root / "s" / vs.SHARD_MANIFEST_FILE).exists()
            return good_parity(plan)
        self.shard(self.root / "s", 0, 3, parity=parity)
        self.assertEqual(seen, {"data_finished": True, "manifest": False})

    def test_a_non_reference_worker_needs_a_passing_parity_certificate(self):
        with self.assertRaisesRegex(vs.ContractError, "parity"):
            self.shard(self.root / "a", 0, 3, parity=None)
        low = lambda p: ({"reference": "t", "samples": 1000, "min_cosine": 0.99, "mean_cosine": 0.999}, None)
        with self.assertRaisesRegex(vs.ContractError, "lowest cosine"):
            self.shard(self.root / "b", 0, 3, parity=low)
        few = lambda p: ({"reference": "t", "samples": 999, "min_cosine": 1.0, "mean_cosine": 1.0}, None)
        with self.assertRaisesRegex(vs.ContractError, "fewer than"):
            self.shard(self.root / "c", 0, 3, parity=few)
        for d in ("a", "b", "c"):
            self.assertFalse((self.root / d / vs.SHARD_MANIFEST_FILE).exists())
        ref = dict(WORKER, ep="cpu", mode="onnxruntime")
        self.assertNotIn("parity", self.shard(self.root / "d", 0, 3, parity=None, worker=ref))

    def test_vectors_that_are_not_finite_unit_or_dim_wide_are_refused(self):
        for bad in ([1.0, 0.0, 0.0], [float("nan"), 0, 0, 1], [2.0, 0, 0, 0]):
            with self.assertRaises(vs.ContractError):
                self.shard(self.root / f"x{len(str(bad))}", 0, 1, embed=lambda t, b=bad: [b] * len(t))

    def test_a_window_past_the_end_holds_what_remains(self):
        m = self.shard(self.root / "s", 9, 10)
        self.assertEqual((m["skip"], m["take"], m["records"]), (9, 10, 2))
        m = self.shard(self.root / "e", 20, 5)
        self.assertEqual(m["records"], 0)


class PackageChecksum(unittest.TestCase):
    def test_the_documented_manifest_is_what_is_hashed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "model.onnx").write_bytes(b"graph bytes")
            (root / "tokenizer.json").write_bytes(b"{}")
            (root / "README.md").write_bytes(b"not covered")
            manifest = ("otzaria-onnx-package-v1\n"
                        f"model.onnx\t11\t{hashlib.sha256(b'graph bytes').hexdigest()}\n"
                        f"tokenizer.json\t2\t{hashlib.sha256(b'{}').hexdigest()}\n")
            self.assertEqual(mp.package_checksum(root, "model.onnx"), hashlib.sha256(manifest.encode()).hexdigest())
            self.assertEqual(mp.tokenizer_checksum(root), hashlib.sha256(b"{}").hexdigest())

    def test_the_meivin_packages_have_the_sidecars_checksums(self):
        tok = ("tokenizer.json", 2191362, "0664287976ecb078bdfd8f5e5515dc87d8cb7f985a79a481aa1cdf7a7321c0e9")
        fp32 = ("seforim-embed-round2-fp32.onnx", 168177986, "1fc2aa8f9e1a85a38c8667b4901205d2cc1f17d1e2e5acc8c676514b2d687948")
        int8 = ("seforim-embed-round2-int8.onnx", 42489219, "659226865abd3a1bc833565ae6b2e2f48abdd7136285824a12966d4d3294cbf8")
        self.assertEqual(mp.checksum_of_entries([tok, fp32]), "4a4a2ae88a86f15ffe6069bfcefc3abd13c207cec5d7aaef52c0c59d752ade46")
        self.assertEqual(mp.checksum_of_entries([int8, tok]), "9e408407922b4aab26dd148cbe4b9a0e573c65591cfc799ba28e991d77d9d065")

    def test_a_package_that_is_not_the_plans_or_not_the_familys_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "g.onnx").write_bytes(b"g")
            (root / "tokenizer.json").write_bytes(b"t")
            good = mp.package_checksum(root, "g.onnx")
            fam = dict(family(256, good), tokenizer_checksum=mp.tokenizer_checksum(root))
            pkg = {"checksum": good, "quantization": "fp32"}
            mp.check_family(root, "g.onnx", fam, pkg, 256, "in-graph", 256)
            with self.assertRaisesRegex(mp.PackageError, "passage_package"):
                mp.check_family(root, "g.onnx", fam, dict(pkg, checksum="1" * 64), 256, "in-graph", 256)
            for args, field in (((255, "in-graph", 256), "embedding_dim"), ((256, "mean", 256), "pooling"),
                                ((256, "in-graph", 128), "max_tokens")):
                with self.assertRaisesRegex(mp.PackageError, field):
                    mp.check_family(root, "g.onnx", fam, pkg, *args)
            with self.assertRaisesRegex(mp.PackageError, "tokenizer_checksum"):
                mp.check_family(root, "g.onnx", dict(fam, tokenizer_checksum="2" * 64), pkg, 256, "in-graph", 256)


class Hub(http.server.BaseHTTPRequestHandler):
    files = {}
    seen = []
    redirect_to = None

    def do_GET(self):
        Hub.seen.append((self.headers.get("Host"), self.path, self.headers.get("Authorization")))
        if self.redirect_to and "/resolve/" in self.path and not self.path.startswith("/cdn"):
            self.send_response(302)
            self.send_header("Location", self.redirect_to + "/cdn" + self.path)
            self.end_headers()
            return
        name = self.path.rsplit("/", 1)[-1]
        if name not in Hub.files:
            self.send_response(404); self.end_headers(); return
        body = Hub.files[name]
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class Fetch(unittest.TestCase):
    def setUp(self):
        Hub.files = {"g.onnx": b"graph", "tokenizer.json": b"tok"}
        Hub.seen = []
        Hub.redirect_to = None
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Hub)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.tmp = tempfile.TemporaryDirectory()
        with tempfile.TemporaryDirectory() as t:
            (Path(t) / "g.onnx").write_bytes(b"graph"); (Path(t) / "tokenizer.json").write_bytes(b"tok")
            self.checksum = mp.package_checksum(Path(t), "g.onnx")
        os.environ["TEST_HF_TOKEN"] = "s3cr3t-token"

    def tearDown(self):
        self.server.shutdown(); self.server.server_close()
        self.tmp.cleanup()
        os.environ.pop("TEST_HF_TOKEN", None)

    def fetch(self, checksum=None, hub=None):
        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            root = mp.fetch_package(Path(self.tmp.name), checksum or self.checksum, "g.onnx", repo="org/model",
                                    revision="main", token_env="TEST_HF_TOKEN",
                                    hub=hub or f"http://127.0.0.1:{self.port}", log=print)
        self.assertNotIn("s3cr3t", out.getvalue())
        return root, out.getvalue()

    def test_the_package_is_fetched_with_the_token_verified_and_then_served_from_the_cache(self):
        root, log = self.fetch()
        self.assertEqual((root / "g.onnx").read_bytes(), b"graph")
        self.assertEqual([s[1] for s in Hub.seen], ["/org/model/resolve/main/g.onnx", "/org/model/resolve/main/tokenizer.json"])
        self.assertTrue(all(s[2] == "Bearer s3cr3t-token" for s in Hub.seen))
        self.assertIn("authenticated", log)
        Hub.seen = []
        self.fetch()
        self.assertEqual(Hub.seen, [])

    def test_a_cached_copy_that_no_longer_hashes_is_fetched_again(self):
        root, _ = self.fetch()
        (root / "g.onnx").write_bytes(b"tampered")
        Hub.seen = []
        self.fetch()
        self.assertEqual(len(Hub.seen), 2)
        self.assertEqual((root / "g.onnx").read_bytes(), b"graph")

    def test_a_package_that_hashes_to_something_else_is_refused_and_removed(self):
        Hub.files["g.onnx"] = b"another graph"
        with self.assertRaisesRegex(mp.PackageError, "refused"):
            self.fetch()
        self.assertFalse((Path(self.tmp.name) / self.checksum / "g.onnx").exists())

    def test_an_http_failure_names_the_url_and_not_the_token(self):
        del Hub.files["tokenizer.json"]
        with self.assertRaises(mp.PackageError) as caught:
            self.fetch()
        self.assertIn("HTTP 404", str(caught.exception))
        self.assertNotIn("s3cr3t", str(caught.exception))

    def test_the_token_is_not_forwarded_when_a_download_is_redirected_to_another_host(self):
        Hub.redirect_to = f"http://localhost:{self.port}"
        self.fetch()
        hub = [s for s in Hub.seen if "/resolve/" in s[1] and not s[1].startswith("/cdn")]
        cdn = [s for s in Hub.seen if s[1].startswith("/cdn")]
        self.assertTrue(hub and cdn)
        self.assertTrue(all(s[2] == "Bearer s3cr3t-token" for s in hub))
        self.assertTrue(all(s[2] is None for s in cdn))


@unittest.skipIf(torch is None, "PyTorch and NumPy are not installed")
class Reimplementation(unittest.TestCase):
    def weights(self, hidden=32, layers=2, out=16, vocab=50, ffn=64, seed=0):
        rng = np.random.default_rng(seed)
        n = lambda *s: (rng.standard_normal(s) * 0.2).astype(np.float32)
        ln = lambda d: (1 + n(d) * 0.1, n(d) * 0.1)
        W = {"word": n(vocab, hidden), "pos": n(64, hidden), "tt": n(1, hidden), "emb_ln": ln(hidden), "layers": []}
        for _ in range(layers):
            L = {r: {"w": n(hidden, hidden), "b": n(hidden)} for r in ("q", "k", "v", "o")}
            L["f1"] = {"w": n(hidden, ffn), "b": n(ffn)}; L["f2"] = {"w": n(ffn, hidden), "b": n(hidden)}
            L["ln1"], L["ln2"] = ln(hidden), ln(hidden)
            W["layers"].append(L)
        W["proj"] = n(hidden, out); W["proj_ln"] = ln(out)
        return W

    def test_a_padded_batch_computes_what_one_text_at_a_time_computes(self):
        import torch_bert as tb
        m = tb.MeivinTorch(self.weights(), mode="fp32", device="cpu", heads=4)
        rng = np.random.default_rng(1)
        ids = [list(rng.integers(0, 50, size=int(k))) for k in (3, 17, 1, 40, 9)]
        batched = tb.embed_id_lists(m, ids)
        single = np.stack([tb.embed_id_lists(m, [x])[0] for x in ids])
        self.assertTrue(np.allclose(batched, single, atol=1e-5))
        self.assertTrue(np.allclose(np.linalg.norm(batched, axis=1), 1, atol=1e-5))
        self.assertEqual(batched.shape, (5, 16))

    def test_batches_are_sorted_by_length_and_fit_the_token_budget(self):
        import torch_bert as tb
        lengths = [5, 300, 17, 64, 1, 256, 33]
        batches = tb.make_batches(lengths, token_budget=512, max_batch=3)
        self.assertEqual(sorted(i for b in batches for i in b), list(range(len(lengths))))
        for b in batches:
            pad = -(-max(lengths[i] for i in b) // tb.PAD_L) * tb.PAD_L
            self.assertTrue(len(b) <= 3 and (len(b) == 1 or pad * len(b) <= 512))

    def test_a_mode_other_than_mixed_or_fp32_is_refused(self):
        import torch_bert as tb
        with self.assertRaises(ValueError):
            tb.MeivinTorch(self.weights(), mode="int8", device="cpu", heads=4)


if __name__ == "__main__":
    unittest.main(verbosity=2)
