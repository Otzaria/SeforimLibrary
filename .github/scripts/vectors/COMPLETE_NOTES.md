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
