# QA491 complete main text and separate notes

This family is opt-in: `all` retains the existing families. Its model packages
are the existing QA491 fp32 passage and int8 query packages; chunking version 3
and embedding text version 4 give it a different artifact identity.

The runner first verifies the database release and its existing index archive.
That index supplies canonical catalogue positions and metadata to
`build_semantic_index`. Every source line is rebuilt from the verified database
into a private `complete-notes-index` directory with
`semantic_projection_version=1`. Published database/index/vector assets are not
replaced. The same global runner lock protects the compatible fp32 warehouse;
verified embeddings of identical exact texts are reused.

To start a full build and all assembly/coverage/resolution/retrieval gates,
without publishing new artifacts:

```sh
gh workflow run build-library-vectors.yml --repo Otzaria/SeforimLibrary \
  --ref otzaria -f release_tag=v38-20261112163847 -f mode=dry-run \
  -f family=meivin-qa491-complete-notes-onnx -f resume=false
```

The work is retained under
`~/otzaria-vectors/work/v38-20261112163847-complete-notes-v1/`.
The pipeline refuses to erase an existing candidate work directory. Completed
shards, their parity certificates and token audit reports remain available for
recovery. A future published base uses the separate prefix
`vectors-qa491-complete-notes`; existing application releases retain their
current vectors.

GPU workers count the complete sequence before applying the original tokenizer's
capped postprocessor, with one tokenization per passage. Each shard retains
`token-audit.json` (and a partial checkpoint while running): total passages,
over-budget passages, discarded token count, longest sequence and bounded
examples. Counts include the role prefix and special tokens. Truncation remains
at the model's declared 256-token cap and is reported explicitly; these reports
do not assert complete coverage after token truncation. The small-plan CPU path
retains the sidecar's equivalent automatic report.

`bootstrap_runner.sh` refuses mismatched sidecar/plugin pins. The pinned plugin
must include `build_semantic_index` and itself pin the same published sidecar.
The model cache must contain the verified QA491 packages. No Kaggle job or legacy
GGUF queue is used by this workflow.

## Imported-build recovery

[Run 44257768773](https://github.com/Otzaria/SeforimLibrary/actions/runs/44257768773)
completed all four GPU shards (3,988,353 new vectors), passed their fp32 parity
checks and imported them into the compatible warehouse. Its original i8 base
was 3,889,391,688 bytes, above the unchanged G7 limit of 3,500,000,000 bytes.
G3/G4/G6 did not run because verification stopped at G7. The original oversized
release is retained as evidence.

[Compact recovery 44291306747](https://github.com/Otzaria/SeforimLibrary/actions/runs/44291306747)
then passed G7 at approximately 3.408 GB and passed G3 coverage and G4
resolution. Its retrieval recall@10 was 0.9835 and recall@50 was 0.987, below
the required 0.99 at 50, so publication stopped. Its output is also retained.

The candidate now returns to the original `i8-sym-vec` encoding. The approved
G7 budget is 4,000,000,000 bytes; actual assembly verification must still pass
that limit. No rotation or neighbor aliases are enabled. This preserves the
original i8 codec's G5 checks. Retrieval requirements remain **recall@10 >= 0.98
and recall@50 >= 0.99**, passed explicitly to the validator. The new i8 recovery
must pass every gate; the earlier compact run does not certify its retrieval.

```sh
gh workflow run build-library-vectors.yml --repo Otzaria/SeforimLibrary \
  --ref otzaria -f release_tag=v38-20261112163847 -f mode=dry-run \
  -f family=meivin-qa491-complete-notes-onnx -f resume=true
```

Recovery verifies the DB against the canonical rebuild's SHA-256, source index
metadata/order, private projection provenance, every frozen plan file (including
book hashes), all complete shard windows/data/parity documents/token audits,
and warehouse identity. It checks every retained vector is finite and unit
length. The imported batch must match both the exact plan and the concatenated
retained shard bytes. Native `warehouse-verify` then rehashes and checks the
warehouse without repair. A mismatch stops recovery without inference,
replanning, downloading, re-importing or deleting retained work. A verification
receipt preserves aggregate token overflow counts.

Each attempt creates a separate
`meivin-qa491-complete-notes-onnx/recovery-i8.<unique>/`
for its fresh i8 assembly and reports. The existing plan, all shards,
private index, DB, original `release/` and previous compact recovery remain unchanged. Assembly gates run
again, followed by full G3 coverage and book/text hash validation, G4 runtime
resolution, and G6 retrieval against the retained private index. Dry-run keeps
all diagnostics and publishes no artifacts. Base mode also requires successful
validation before the existing publication path; retained recovery inputs are
preserved in that mode too. Pardes's embedding-stage recovery retains its
separate policy.
