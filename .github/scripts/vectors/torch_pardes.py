"""Auditable torch-only Pardes student architecture; no downloaded code import.

Matches the pinned NeoBERT source's SDPA path, including its unusual head-local
QKV layout and half-pair RoPE. Pure FP32 dequantization is an approximate native
derivative. This module must pass independent parity review before execution.
"""
import hashlib
import json
from pathlib import Path

import numpy as np
import torch
from torch import nn
from torch.nn import functional as F


class RMSNorm(nn.Module):
    def __init__(self, hidden_size=768, eps=1e-6):
        super().__init__()
        self.weight = nn.Parameter(torch.ones(hidden_size))
        self.eps = eps

    def forward(self, x):
        dtype = x.dtype
        v = x.float()
        return self.weight * (v * torch.rsqrt(v.pow(2).mean(-1, keepdim=True) + self.eps)).to(dtype)


class SwiGLU(nn.Module):
    def __init__(self, hidden_size=768, intermediate_size=3072):
        super().__init__()
        self.w12 = nn.Linear(hidden_size, 2 * intermediate_size, bias=False)
        self.w3 = nn.Linear(intermediate_size, hidden_size, bias=False)

    def forward(self, x):
        gate, value = self.w12(x).chunk(2, dim=-1)
        return self.w3(F.silu(gate) * value)


class EncoderBlock(nn.Module):
    def __init__(self):
        super().__init__()
        self.qkv = nn.Linear(768, 2304, bias=False)
        self.wo = nn.Linear(768, 768, bias=False)
        self.attention_norm = RMSNorm()
        self.ffn_norm = RMSNorm()
        self.ffn = SwiGLU()

    def forward(self, x, key_mask, cosine, sine):
        batch, tokens, _ = x.shape
        # Q,K,V are grouped INSIDE each head, rather than Q-all-heads first.
        q, k, v = self.qkv(self.attention_norm(x)).view(batch, tokens, 12, 192).chunk(3, dim=-1)
        cosine, sine = cosine[None, :, None, :].to(q.dtype), sine[None, :, None, :].to(q.dtype)
        q1, q2 = q[..., :32], q[..., 32:]
        k1, k2 = k[..., :32], k[..., 32:]
        q = torch.cat((q1 * cosine - q2 * sine, q1 * sine + q2 * cosine), dim=-1)
        k = torch.cat((k1 * cosine - k2 * sine, k1 * sine + k2 * cosine), dim=-1)
        value = F.scaled_dot_product_attention(q.transpose(1, 2), k.transpose(1, 2), v.transpose(1, 2),
                                              attn_mask=key_mask, dropout_p=0.0).transpose(1, 2)
        x = x + self.wo(value.reshape(batch, tokens, 768))
        return x + self.ffn(self.ffn_norm(x))


class Backbone(nn.Module):
    def __init__(self, cosine, sine):
        super().__init__()
        self.encoder = nn.Embedding(128000, 768, padding_idx=3)
        self.transformer_encoder = nn.ModuleList(EncoderBlock() for _ in range(7))
        self.layer_norm = RMSNorm()
        self.register_buffer("freqs_cos", cosine, persistent=False)
        self.register_buffer("freqs_sin", sine, persistent=False)

    def forward(self, ids, mask):
        assert ids.ndim == mask.ndim == 2 and ids.shape == mask.shape
        assert ids.shape[1] <= 4096
        x = self.encoder(ids)
        key_mask = mask[:, None, None, :].bool()
        cosine, sine = self.freqs_cos[:ids.shape[1]], self.freqs_sin[:ids.shape[1]]
        for block in self.transformer_encoder:
            x = block(x, key_mask, cosine, sine)
        return self.layer_norm(x)


class RecoveredPardesStudent(nn.Module):
    def __init__(self, cosine, sine):
        super().__init__()
        self.base = Backbone(cosine, sine)
        self.dense = nn.Linear(768, 384, bias=False)

    def forward(self, input_ids, attention_mask):
        hidden = self.base(input_ids, attention_mask)
        mask = attention_mask.unsqueeze(-1).to(hidden.dtype)
        pooled = (hidden * mask).sum(1) / mask.sum(1).clamp(min=1e-9)
        return F.normalize(self.dense(pooled), p=2, dim=-1)


def load_weights(path):
    """Read exact released graph tensors; no independent checkpoint or remote code."""
    import onnx
    from onnx import numpy_helper
    p = Path(path)
    with p.open('rb') as f:
        if hashlib.file_digest(f, 'sha256').hexdigest() != 'd54f445ee08e0af8eb727f5146a7373a97badbb2ccb7b4862823573ac438e108':
            raise ValueError('Requires exact verified P107 FP32 graph')
    graph = onnx.load(p, load_external_data=False)
    if any(t.data_location == onnx.TensorProto.EXTERNAL for t in graph.graph.initializer):
        raise ValueError('External tensors not supported')
    inits = {i.name: numpy_helper.to_array(i).copy() for i in graph.graph.initializer}
    nodes = {o: n for n in graph.graph.node for o in n.output}
    weights = {k: torch.from_numpy(v) for k, v in inits.items() if not k.startswith('onnx::')}
    paths = [('base.transformer_encoder.%d.%s.weight' % (i, name),
              '/base/transformer_encoder.%d/%s/MatMul_output_0' % (i, name.replace('.', '/')))
             for i in range(7) for name in ('qkv', 'wo', 'ffn.w12', 'ffn.w3')]
    paths.append(('dense.weight', '/dense/MatMul_output_0'))
    for key, output in paths:
        n = nodes[output]
        if n.op_type != 'MatMul' or n.input[1] not in inits: raise ValueError('Unexpected projection')
        weights[key] = torch.from_numpy(inits[n.input[1]].T.copy())
    constants = {}
    for label, name in [('cosine', '/base/Constant_3_output_0'), ('sine', '/base/Constant_8_output_0')]:
        node = nodes[name]
        if node.op_type != 'Constant': raise ValueError('Missing fixed RoPE tensor')
        arr = numpy_helper.to_array(next(a.t for a in node.attribute if a.name == 'value')).copy()
        if arr.shape != (4096, 32): raise ValueError('Wrong RoPE shape')
        constants[label] = torch.from_numpy(arr)
    if len(weights) != 45 or any(t.dtype != torch.float32 or not torch.isfinite(t).all() for t in weights.values()):
        raise ValueError('Wrong weight inventory')
    return weights, constants

class PardesTorch:
    def __init__(self, weights, mode='fp32', device='cuda'):
        if mode != 'fp32': raise ValueError('Only FP32 Pardes execution is qualified')
        state, rope = weights
        self.dev = torch.device(device)
        self.dim = 384
        self.pad_id = 3
        self.model = RecoveredPardesStudent(rope['cosine'], rope['sine'])
        self.model.load_state_dict(state, strict=True)
        self.model.to(self.dev).eval()

    @torch.inference_mode()
    def forward(self, ids, lengths):
        mask = torch.arange(ids.shape[1], device=ids.device)[None, :] < lengths[:, None]
        return self.model(ids, mask.to(torch.int64))
