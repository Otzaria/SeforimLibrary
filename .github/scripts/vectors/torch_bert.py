"""A batched PyTorch re-implementation of the reviewed Meivin fp32 ONNX exports.

The weights are read straight from the package's ONNX file (its initializers), never
re-exported. The graph's own structure is followed node for node: word + token-type +
position embeddings and LayerNorm (eps 1e-12); per layer Q/K/V projections, Q and K each
scaled by head_dim**-0.25 (the graph's 0.35355338 for 64), softmax over -inf-masked keys,
output projection, residual LayerNorm, erf GELU (the fused form ONNX Runtime turns the
graph's Div/Erf/Add/Mul/Mul into), FFN, residual LayerNorm; mean pooling over real tokens
with Clip(min=1); a bias-free projection; LayerNorm (eps 1e-5); L2 with Clip(min=1e-12).

Modes:
  fp32   every GEMM in fp32
  mixed  GEMM inputs rounded to fp16, products accumulated and returned in fp32
         (torch.mm / torch.bmm out_dtype); everything else fp32. The production mode.

Padding: rows are padded to a multiple of PAD_L with masked keys and masked pooling, so a
batch computes what batch-1 runs of the graph compute.
"""

from __future__ import annotations

import numpy as np
import re
import torch

PAD_L = 16
GRAPH_FP32 = "seforim-embed-round2-fp32.onnx"
ROLES = {
    "q": "attention/self/query", "k": "attention/self/key", "v": "attention/self/value",
    "o": "attention/output/dense", "f1": "intermediate/dense", "f2": "output/dense",
}


def load_weights(path: str) -> dict:
    """The fp32 graph's initializers, arranged per layer (the MatMul weights found by the
    output names of the nodes that consume them)."""
    import onnx
    from onnx import numpy_helper
    m = onnx.load(path)
    inits = {i.name: numpy_helper.to_array(i) for i in m.graph.initializer}
    by_out = {o: n for n in m.graph.node for o in n.output}

    def matmul(out_name):
        n = by_out[out_name]
        if n.op_type != "MatMul":
            raise ValueError(f"{out_name} is produced by {n.op_type}, not MatMul: not the fp32 graph")
        return inits[n.input[1]]

    # The reviewed exports differ only in the outer module's namespace.
    prefixes = [p for p in ("encoder.", "") if p + "backbone.embeddings.word_embeddings.weight" in inits]
    if len(prefixes) != 1:
        raise ValueError("expected exactly one supported fp32 BERT namespace")
    outer = prefixes[0]
    node_prefix = "/" + outer.replace(".", "/")
    p = outer + "backbone."
    W = {
        "word": inits[p + "embeddings.word_embeddings.weight"],
        "pos": inits[p + "embeddings.position_embeddings.weight"],
        "tt": inits[p + "embeddings.token_type_embeddings.weight"],
        "emb_ln": (inits[p + "embeddings.LayerNorm.weight"], inits[p + "embeddings.LayerNorm.bias"]),
        "layers": [],
    }
    i = 0
    while f"{node_prefix}backbone/encoder/layer.{i}/attention/self/query/MatMul_output_0" in by_out:
        lp, ip = f"{node_prefix}backbone/encoder/layer.{i}/", p + f"encoder.layer.{i}."
        layer = {r: {"w": matmul(lp + role + "/MatMul_output_0"),
                     "b": inits[ip + role.replace("/", ".") + ".bias"]} for r, role in ROLES.items()}
        layer["ln1"] = (inits[ip + "attention.output.LayerNorm.weight"], inits[ip + "attention.output.LayerNorm.bias"])
        layer["ln2"] = (inits[ip + "output.LayerNorm.weight"], inits[ip + "output.LayerNorm.bias"])
        W["layers"].append(layer)
        i += 1
    if not W["layers"]:
        raise ValueError("the fp32 BERT graph contains no complete attention layer")
    declared_layers = {int(match.group(1)) for name in inits
                       if (match := re.fullmatch(re.escape(p) + r"encoder\.layer\.(\d+)\.attention\.self\.query\.bias", name))}
    if declared_layers != set(range(i)):
        raise ValueError("the fp32 BERT graph has an incomplete or non-contiguous attention stack")
    W["proj"] = matmul(node_prefix + "projection/MatMul_output_0")
    W["proj_ln"] = (inits[outer + "projection_norm.weight"], inits[outer + "projection_norm.bias"])
    for n in m.graph.node:
        if n.op_type == "LayerNormalization":
            expected = 1e-5 if n.output[0] == node_prefix + "projection_norm/LayerNormalization_output_0" else 1e-12
            eps = next((a.f for a in n.attribute if a.name == "epsilon"), 1e-5)
            if not np.isclose(eps, expected, rtol=1e-6, atol=0):
                raise ValueError(f"{n.output[0]} has unsupported LayerNorm epsilon {eps}")
    return W


class MeivinTorch:
    def __init__(self, W: dict, mode: str = "mixed", device: str = "cuda", heads: int = 8):
        if mode not in ("mixed", "fp32"):
            raise ValueError(f"mode {mode!r}: this worker runs 'mixed' or 'fp32'")
        self.mode, self.dev, self.heads = mode, torch.device(device), heads
        t = lambda a: torch.from_numpy(np.array(a, dtype=np.float32, copy=True)).to(self.dev)
        self.word, self.pos, self.tt = t(W["word"]), t(W["pos"]), t(W["tt"])
        self.hidden = self.word.shape[1]
        if self.hidden % heads:
            raise ValueError(f"hidden width {self.hidden} is not divisible by {heads} heads")
        self.head_dim = self.hidden // heads
        # the graph scales Q and K each by head_dim ** -0.25, as float32 (0.35355338 for 64)
        self.scale = float(np.float32(self.head_dim ** -0.25))
        self.emb_ln = tuple(t(x) for x in W["emb_ln"])
        self.layers = []
        for layer in W["layers"]:
            L = {}
            for r in ("o", "f1", "f2"):
                L[r] = {"w": t(layer[r]["w"]), "b": t(layer[r]["b"])}
            L["qkv"] = {"w": t(np.concatenate([layer[r]["w"] for r in "qkv"], axis=1)),
                        "b": t(np.concatenate([layer[r]["b"] for r in "qkv"]))}
            for d in [L["o"], L["f1"], L["f2"], L["qkv"]]:
                d["wh"] = d["w"].half()
            L["ln1"] = tuple(t(x) for x in layer["ln1"])
            L["ln2"] = tuple(t(x) for x in layer["ln2"])
            self.layers.append(L)
        self.proj = {"w": t(W["proj"])}
        self.proj["wh"] = self.proj["w"].half()
        self.proj_ln = tuple(t(x) for x in W["proj_ln"])
        self.dim = self.proj["w"].shape[1]

    def _mm(self, x2, p):
        if self.mode == "mixed":
            return torch.mm(x2.half(), p["wh"], out_dtype=torch.float32)
        return torch.mm(x2, p["w"])

    def _bmm(self, a, b):
        if self.mode == "mixed":
            return torch.bmm(a.half(), b.half(), out_dtype=torch.float32)
        return torch.bmm(a, b)

    @torch.no_grad()
    def forward(self, ids: torch.Tensor, lengths: torch.Tensor) -> torch.Tensor:
        B, L = ids.shape
        H, DH, D = self.heads, self.head_dim, self.hidden
        maskb = torch.arange(L, device=self.dev)[None, :] < lengths[:, None]
        x = self.word[ids] + self.tt[0]
        x = x + self.pos[:L][None]
        x2 = torch.nn.functional.layer_norm(x, (D,), *self.emb_ln, eps=1e-12).reshape(B * L, D)
        key_add = torch.where(maskb, 0.0, float("-inf")).to(torch.float32)[:, None, None, :]
        for Lp in self.layers:
            qkv = Lp["qkv"]["b"] + self._mm(x2, Lp["qkv"])
            q = qkv[:, :D].reshape(B, L, H, DH).transpose(1, 2) * self.scale
            kT = qkv[:, D:2 * D].reshape(B, L, H, DH).permute(0, 2, 3, 1) * self.scale
            v = qkv[:, 2 * D:].reshape(B, L, H, DH).transpose(1, 2)
            s = self._bmm(q.reshape(B * H, L, DH), kT.reshape(B * H, DH, L)).view(B, H, L, L) + key_add
            p = torch.softmax(s, dim=-1)
            c = self._bmm(p.reshape(B * H, L, L), v.reshape(B * H, L, DH)).view(B, H, L, DH)
            c2 = c.transpose(1, 2).reshape(B * L, D)
            a = Lp["o"]["b"] + self._mm(c2, Lp["o"]) + x2
            x2 = torch.nn.functional.layer_norm(a, (D,), *Lp["ln1"], eps=1e-12)
            h = torch.nn.functional.gelu(Lp["f1"]["b"] + self._mm(x2, Lp["f1"]))
            o = Lp["f2"]["b"] + self._mm(h, Lp["f2"]) + x2
            x2 = torch.nn.functional.layer_norm(o, (D,), *Lp["ln2"], eps=1e-12)
        m = maskb.to(torch.float32)[:, :, None]
        pooled = (x2.view(B, L, D) * m).sum(1) / torch.clamp(m.sum(1), min=1.0)
        pr = self._mm(pooled, self.proj)
        pr = torch.nn.functional.layer_norm(pr, (self.dim,), *self.proj_ln, eps=1e-5)
        n = torch.linalg.vector_norm(pr, dim=1, keepdim=True)
        return pr / torch.clamp(n, min=1e-12)


def make_batches(lengths, token_budget=32768, max_batch=1024):
    """Indices sorted by length (descending), cut into batches whose padded size fits the budget."""
    order = np.argsort(-np.asarray(lengths), kind="stable")
    batches, cur, cur_max = [], [], 0
    for i in order:
        li = int(-(-lengths[i] // PAD_L) * PAD_L)
        new_max = max(cur_max, li)
        if cur and (new_max * (len(cur) + 1) > token_budget or len(cur) >= max_batch):
            batches.append(cur)
            cur, new_max = [], li
        cur.append(int(i))
        cur_max = new_max
    if cur:
        batches.append(cur)
    return batches


def embed_id_lists(model: MeivinTorch, id_lists, token_budget=32768, max_batch=1024) -> np.ndarray:
    lengths = np.array([len(x) for x in id_lists])
    out = np.empty((len(id_lists), model.dim), dtype=np.float32)
    for b in make_batches(lengths, token_budget, max_batch):
        Lp = -(-int(lengths[b].max()) // PAD_L) * PAD_L
        arr = np.zeros((len(b), Lp), dtype=np.int64)
        for j, i in enumerate(b):
            arr[j, : lengths[i]] = id_lists[i]
        ids = torch.from_numpy(arr).to(model.dev)
        lens = torch.from_numpy(lengths[b].astype(np.int64)).to(model.dev)
        out[b] = model.forward(ids, lens).float().cpu().numpy()
    return out
