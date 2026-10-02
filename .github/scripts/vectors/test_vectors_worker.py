"""CPU tests for the vector worker: the v2 shard contract, the package checksum and fetch,
the bridge, and (when PyTorch is installed) the re-implementation's batching.

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
import types
import unittest
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import model_package as mp  # noqa: E402
import vector_shard as vs  # noqa: E402

try:
    import numpy as np
except ImportError:  # the CI image has neither NumPy nor PyTorch; their tests skip
    np = None
try:
    import torch
except ImportError:
    torch = None

DIM = 4
PINS = dict(line.split("=", 1) for line in (HERE / "pins.env").read_text().splitlines()
            if line and not line.startswith("#") and "=" in line)


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


GOLDEN = json.loads((HERE / "parity_golden_texts.json").read_text(encoding="utf-8"))["cases"]
SMALL_PLANS = ((1, False), (100, False), (958, False), (959, True))   # texts, and whether a GPU shard can be certified


class ParitySample(unittest.TestCase):
    """The real sample selection and acceptance rule, with no stand-in certificate."""

    def test_a_certificate_reaches_1000_samples_only_from_a_plan_of_959_texts(self):
        need = vs.PARITY_MIN_SAMPLES - len(GOLDEN)
        self.assertEqual(need, 959)
        for records, certifiable in SMALL_PLANS:
            samples = len(GOLDEN) + len(vs.sample_positions("ab" * 32, records, need))
            self.assertEqual(samples, len(GOLDEN) + min(records, need))
            parity = {"samples": samples, "min_cosine": 1.0}
            if certifiable:
                vs.check_parity(parity)
            else:
                with self.assertRaisesRegex(vs.ContractError, "fewer than 1000"):
                    vs.check_parity(parity)


@unittest.skipIf(np is None, "NumPy is not installed")
class WorkerOnSmallPlans(unittest.TestCase):
    """embed_worker.main on plans of 1, 100, 958 and 959 texts: its own sample selection, parity
    certificate and shard writer. Only the numbers are stood in for: the GPU and the ONNX Runtime
    reference both return the same unit vectors, so parity is perfect."""

    def run_worker(self, texts):
        import embed_worker
        loads = []
        unit = lambda ids: np.tile(np.array([[1.0, 0.0, 0.0, 0.0]], dtype=np.float32), (len(ids), 1))
        torch = types.SimpleNamespace(backends=types.SimpleNamespace(cuda=types.SimpleNamespace(matmul=types.SimpleNamespace())),
                                      version=types.SimpleNamespace(hip=None))
        torch_bert = types.SimpleNamespace(load_weights=lambda path: loads.append(path) or {},
                                           MeivinTorch=lambda *a, **k: types.SimpleNamespace(dim=DIM),
                                           embed_id_lists=lambda model, ids, *a: unit(ids))
        tok = types.SimpleNamespace(encode_batch=lambda texts, **k: [types.SimpleNamespace(ids=[1, 2]) for _ in texts])
        reference = types.SimpleNamespace(load_tokenizer=lambda *a: tok, version=lambda: "stand-in",
                                          embed_reference=lambda graph, ids, procs: unit(ids),
                                          cosines=lambda a, b: np.einsum("ij,ij->i", a, b))
        with tempfile.TemporaryDirectory() as t, \
                mock.patch.dict(sys.modules, {"torch": torch, "torch_bert": torch_bert, "reference": reference}), \
                mock.patch.object(embed_worker.model_package, "check_family", lambda *a: None):
            root = Path(t)
            fam = family()
            vs.write_embed_plan(root / "plan", [f"[PASSAGE] text {i}" for i in range(texts)], fam, fam["query_packages"][1])
            err = io.StringIO()
            try:
                with contextlib.redirect_stderr(err):
                    embed_worker.main(["--plan", str(root / "plan"), "--out", str(root / "shard"),
                                       "--model-dir", str(root), "--device", "cpu"])
                error = None
            except vs.ContractError as e:
                error = str(e)
            shard = root / "shard"
            files = sorted(p.name for p in shard.iterdir()) if shard.exists() else []
            manifest = json.loads((shard / vs.SHARD_MANIFEST_FILE).read_text()) if vs.SHARD_MANIFEST_FILE in files else None
            return error, files, manifest, loads

    def test_a_plan_its_sample_cannot_certify_is_refused_before_any_work(self):
        for texts, certifiable in SMALL_PLANS:
            if certifiable:
                continue
            with self.subTest(texts=texts):
                error, files, manifest, loads = self.run_worker(texts)
                self.assertIsNotNone(error)
                self.assertIn(f"{texts} text(s)", error)
                self.assertIn("embed-shard", error)     # where a plan this small goes instead
                self.assertEqual((files, manifest, loads), ([], None, []))   # nothing loaded, nothing written

    def test_a_plan_of_959_texts_is_certified_with_1000_samples_and_its_shard_written(self):
        error, files, manifest, loads = self.run_worker(959)
        self.assertIsNone(error)
        self.assertEqual(len(loads), 1)
        self.assertEqual(manifest["records"], 959)
        self.assertEqual(manifest["parity"]["samples"], 1000)
        self.assertEqual(manifest["parity"]["min_cosine"], 1.0)
        self.assertIn(vs.PARITY_DOCUMENT_FILE, files)


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

    def fetch(self, checksum=None, hub=None, **revision):
        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            root = mp.fetch_package(Path(self.tmp.name), checksum or self.checksum, "g.onnx", repo="org/model",
                                    token_env="TEST_HF_TOKEN", hub=hub or f"http://127.0.0.1:{self.port}", log=print,
                                    **(revision or {"revision": "main"}))
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

    def test_by_default_the_package_is_fetched_at_the_pinned_mirror_revision(self):
        self.assertRegex(PINS["MODEL_REVISION"], r"^[0-9a-f]{7,40}$")
        self.assertEqual(mp.DEFAULT_REPO, PINS["MODEL_REPO"])
        self.assertEqual(mp.DEFAULT_REVISION, PINS["MODEL_REVISION"])
        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            mp.fetch_package(Path(self.tmp.name), self.checksum, "g.onnx", repo="org/model", token_env="TEST_HF_TOKEN",
                             hub=f"http://127.0.0.1:{self.port}")
        rev = PINS["MODEL_REVISION"]
        self.assertEqual([s[1] for s in Hub.seen], [f"/org/model/resolve/{rev}/g.onnx", f"/org/model/resolve/{rev}/tokenizer.json"])

    def run_cli(self, *extra):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = mp.main(["fetch", "--cache", self.tmp.name, "--checksum", self.checksum, "--graph", "g.onnx",
                          "--repo", "org/model", "--token-env", "TEST_HF_TOKEN", "--hub", f"http://127.0.0.1:{self.port}",
                          *extra])
        self.assertNotIn("s3cr3t", out.getvalue() + err.getvalue())
        return rc, out.getvalue(), err.getvalue()

    def test_the_command_line_fetch_prints_the_package_directory_and_nothing_else(self):
        rc, out, err = self.run_cli("--revision", "abc1234")
        self.assertEqual(rc, 0, err)
        self.assertEqual(out, str(Path(self.tmp.name) / self.checksum) + "\n")
        self.assertEqual([s[1] for s in Hub.seen], ["/org/model/resolve/abc1234/g.onnx", "/org/model/resolve/abc1234/tokenizer.json"])

    def test_the_command_line_fetch_fails_with_status_2_and_says_why(self):
        del Hub.files["tokenizer.json"]
        rc, out, err = self.run_cli()
        self.assertEqual((rc, out), (2, ""))
        self.assertIn("HTTP 404", err)

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


class Bridge(unittest.TestCase):
    def test_a_whole_library_run_becomes_one_verified_window_with_hard_links(self):
        import bridge_whole_library as bridge
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "run"; run.mkdir()
            fam = family(DIM, "c" * 64)
            vecs = b"".join(struct.pack("<4f", *fake_vector(t)) for t in TEXTS)
            (run / "vectors.f32").write_bytes(vecs)
            (run / "keys.sha256").write_bytes(b"".join(hashlib.sha256(t.encode()).digest() for t in TEXTS))
            (run / "certificate.json").write_text(json.dumps({"pass": True,
                "goldens": {"cases": 41, "min_cos": 0.9999998, "mean_cos": 0.9999999},
                "ort_crosscheck": {"n": 20480, "min": 0.9999997, "mean": 0.99999986,
                                   "ort": "onnxruntime 1.28.0 CPU fp32 graph, 16 proc x 1 thread"}}))
            (run / "manifest.json").write_text(json.dumps({
                "model": {"passage_package": {"package_checksum": "c" * 64, "variant": "fp32"}},
                "worker": {"device": "AMD Radeon RX 9060 XT"}, "vectors": {"count": len(TEXTS)},
                "files": {"vectors.f32": {"sha256": hashlib.sha256(vecs).hexdigest()}}}))
            (root / "unique.jsonl").write_text("".join(json.dumps({"k": hashlib.sha256(t.encode()).hexdigest(), "t": t},
                                                                  ensure_ascii=False) + "\n" for t in TEXTS), encoding="utf-8")
            (root / "family.json").write_text(json.dumps(fam))
            args = ["--run", str(run), "--unique", str(root / "unique.jsonl"), "--family", str(root / "family.json"),
                    "--out", str(root / "shard"), "--plan-out", str(root / "plan")]
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(bridge.main(args), 0)
            m = json.loads((root / "shard" / vs.SHARD_MANIFEST_FILE).read_text())
            plan = vs.read_embed_manifest(root / "plan")
            self.assertEqual((m["skip"], m["take"], m["records"]), (0, len(TEXTS), len(TEXTS)))
            self.assertEqual(m["plan_sha256"], plan["plan_sha256"])
            self.assertEqual(m["plan_sha256"], vs.sha256_file(root / "plan" / vs.EMBED_PLAN_FILE))
            self.assertEqual(os.stat(root / "shard" / vs.VECTORS_FILE).st_ino, os.stat(run / "vectors.f32").st_ino)
            self.assertEqual(m["parity"]["samples"], 20521)
            self.assertEqual(m["parity"]["reference"], "onnxruntime 1.28.0 cpu fp32")
            self.assertEqual(m["parity"]["min_cosine"], 0.9999997)
            self.assertEqual(m["worker"]["mode"], "torch-mixed")
            self.assertEqual([r.text for r in vs.read_window(root / "plan", 0, 99)], TEXTS)
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(vs.ContractError):
                bridge.main(args)  # the finished shard is not overwritten
            keys = bytearray((run / "keys.sha256").read_bytes()); keys[40] ^= 1
            (run / "keys.sha256").write_bytes(bytes(keys))
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(vs.ContractError, "disagree"):
                bridge.main(args[:-4] + ["--out", str(root / "shard2")])


@unittest.skipIf(torch is None or np is None, "PyTorch and NumPy are not installed")
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
