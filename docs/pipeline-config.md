# Pipeline Config

## Purpose

The repository exposes a strict, versioned pipeline configuration surface so external tooling can run controlled comparisons without editing code.

This repo does **not** implement sweeps, CV splits, benchmark scoring, or tuning loops. It only:

- validates pipeline config,
- executes the pipeline,
- persists config provenance,
- fails loudly on unsupported settings or artifact/config mismatches.

This config surface is intentionally separate from base runtime settings loaded from the repo-root `.env` file:

- `.env` covers things like database URL, storage root, API key, and base response/embedding model defaults
- pipeline config covers typed indexing, retrieval, packing, staged generation, and strict-eval behavior

## Schema

The current schema version is `rfx_pipeline.v1`.

Top-level sections:

- `pipeline_version`
- `indexing`
- `retrieval`
- `packing`
- `models`
- `generation`
- `revision`

Unknown fields are rejected. Invalid enum values are rejected. Unsupported non-default values are rejected explicitly.

The committed default profile lives at [`backend/app/pipeline/default_profile.json`](../backend/app/pipeline/default_profile.json).

## Default behavior

If no config is supplied, the repo resolves the committed `default` profile and preserves the current product behavior.

Current default behavior includes:

- index with the runtime default embedding model from settings
- case-profile extraction defaults to `RFX_OPENAI_RESPONSE_MODEL` from settings with `low` reasoning effort
- answer planning defaults to `RFX_OPENAI_RESPONSE_MODEL` from settings with `low` reasoning effort
- answer rendering defaults to `RFX_OPENAI_RESPONSE_MODEL` from settings with `low` reasoning effort
- current PDF chunking stays on the legacy deterministic char-based chunker
- default current PDF chunk unit is `legacy_char`
- default current PDF chunking remains non-contextualized
- historical case signatures default to `summary_plus_analysis_items`
- retrieval broadening stays enabled with the current single broadened stage
- dedup stays on provenance-based dedup
- evidence block order stays:
  - `current_case_facts`
  - `raw_current_pdf`
  - `product_truth`
  - `historical_exemplars`
- revision classifier stays `revision_classifier.v2`
- style-only revisions reuse retrieval

## Index-time vs runtime config

This distinction is enforced.

### Index-time config

These settings affect persisted/indexed artifacts and therefore require explicit import/rebuild when changed:

- `indexing.embedding_model`
- `indexing.current_pdf.*`
- `indexing.historical.signature_mode`
- `models.case_profile_extraction.*`

Artifacts persist artifact-relevant compatibility hashes. Requests using an incompatible index config fail loudly. The repo will not silently rebuild artifacts during draft or bulk-fill requests.

### Runtime config

These settings affect retrieval, packing, or generation at request time and can be overridden safely when compatible with the existing index artifacts:

- `retrieval.*`
- `packing.*`
- `models.answer_planning.*`
- `models.answer_rendering.*`
- `generation.*`
- `revision.*`

Runtime overrides produce a different `pipeline_config_hash` and `runtime_config_hash` but can reuse existing indexed artifacts when the `index_config_hash` still matches.

## Active knobs

These knobs are wired today and have real effect.

### Index-time

- `indexing.embedding_model`
- `indexing.current_pdf.chunk_unit`
- `indexing.current_pdf.chunk_size`
- `indexing.current_pdf.chunk_overlap`
- `indexing.current_pdf.contextualize_chunks`
- `indexing.historical.signature_mode`

### Runtime retrieval

- `retrieval.query_weights.row_context`
- `retrieval.query_weights.row_question`
- `retrieval.query_weights.user_message`
- `retrieval.query_weights.current_case_signature`
- `retrieval.query_weights.row_question_context`
- `retrieval.scoring.keyword_weight`
- `retrieval.scoring.semantic_weight`
- `retrieval.scoring.historical_row_weight`
- `retrieval.scoring.historical_case_weight`
- `retrieval.scoring.same_language_bonus`
- `retrieval.candidate_pool.current_case_facts`
- `retrieval.candidate_pool.current_pdf_evidence`
- `retrieval.candidate_pool.product_truth`
- `retrieval.candidate_pool.historical_exemplars`
- `retrieval.final_quota.current_case_facts`
- `retrieval.final_quota.current_pdf_evidence`
- `retrieval.final_quota.product_truth`
- `retrieval.final_quota.historical_exemplars`
- `retrieval.sufficiency.threshold`
- `retrieval.broadening.enabled`
- `retrieval.broadening.max_stages`
- `retrieval.dedup.mode`

### Runtime packing

- `packing.max_context_tokens`
- `packing.order_strategy`
- `packing.source_block_order`

Current packing semantics:

- evidence is first normalized, then packed immediately before answer planning
- `source_block_order` controls block ordering across:
  - `current_case_facts`
  - `raw_current_pdf`
  - `product_truth`
  - `historical_exemplars`
- retrieval rank is preserved within each block
- `max_context_tokens`, when set, truncates deterministically at evidence-item boundaries over the packed normalized-evidence payload sent to answer planning
- `max_context_tokens` requires a tokenizer-known answer-planning model; unsupported tokenizer resolution fails validation loudly instead of approximating

### Runtime generation

- `generation.temperature`
- `generation.target_answer_words_min`
- `generation.target_answer_words_max`

### Stage-specific model selection

- `models.case_profile_extraction.model_id`
- `models.case_profile_extraction.reasoning_effort`
- `models.answer_planning.model_id`
- `models.answer_planning.reasoning_effort`
- `models.answer_rendering.model_id`
- `models.answer_rendering.reasoning_effort`

These three stages now map to:

- extraction core generation
- internal `AnswerPlan.v2`
- final customer-facing rendering

## Accepted but fixed/default-only values

These fields exist but remain constrained. Unsupported non-default use fails validation.

- non-default `indexing.current_pdf.chunk_size` or `chunk_overlap` while `chunk_unit=legacy_char`
- `retrieval.broadening.max_stages` above `1`
- `packing.order_strategy` values other than `source_block_order`
- `revision.classifier_version` values other than `revision_classifier.v2`
- `revision.style_only_reuses_previous_snapshot=false`

The revision section is versioned and persisted, but the current implementation intentionally keeps the runtime behavior fixed to the existing semantics.

### Legacy compatibility aliases

The repo still accepts:

- `generation.model_id`
- `generation.reasoning_effort`

Current behavior is narrower than the stage-specific surface:

- `generation.model_id` remains a legacy compatibility field. When answer-planning and answer-rendering still use the committed default model IDs, this legacy field backfills those stages.
- `generation.reasoning_effort` is still parsed, but the committed default profile already pins stage reasoning settings. In practice, conflicting non-default values fail validation.
- Prefer `models.answer_planning.*` and `models.answer_rendering.*` on the primary stable surface.
- If both surfaces are supplied with conflicting values, validation fails loudly.

### Current-PDF chunking modes

- `chunk_unit=legacy_char`
  - preserves the exact legacy default behavior
  - requires the default `chunk_size=900` and `chunk_overlap=150`
- `chunk_unit=token`
  - uses deterministic token-aware chunking with the embedding-model tokenizer
  - `chunk_size` and `chunk_overlap` are active
  - `chunk_overlap` must be smaller than `chunk_size`
- `contextualize_chunks=true`
  - prepends deterministic case/client/page metadata to stored chunk content before hashing and embedding
  - changes chunk text, chunk hashes, embeddings, and chunking version

Default behavior remains `chunk_unit=legacy_char` and `contextualize_chunks=false`.

## Artifact-specific compatibility hashes

The repo keeps the full selected `index_config_hash` for case/job provenance, but compatibility checks use artifact-relevant hashes:

- live case PDF chunks: `embedding_model + current_pdf`
- live case-profile artifacts: `embedding_model + case_profile_extraction`
- historical corpus artifacts: `embedding_model + historical + case_profile_extraction`
- product-truth artifacts: `embedding_model`

This prevents unrelated sweeps, such as current-PDF chunking changes, from forcing unnecessary historical or product-truth rebuilds.

Embedding-model lineage remains intentionally coupled across artifact families:

- the relevant compatibility hash for every semantic corpus still includes `embedding_model`
- changing `embedding_model` requires rebuilding/reimporting all semantic corpora
- retrieval ranks candidates within each corpus and applies per-corpus quotas before assembling the final evidence pack; it does not treat all corpus scores as one unrestricted global nearest-neighbor pool

## Intentionally omitted for now

These knobs are not part of the active surface yet because the repo does not implement them safely:

- separate historical style-exemplar quotas
- `packing.style_exemplar_count`
- semantic/near-duplicate dedup modes beyond provenance/off
- alternate revision classifier modes

## Config precedence

The resolved config follows this order:

1. explicit request/job override
2. pinned case/job config when present
3. named committed profile
4. compiled-in fallback equal to the committed default profile

The current repo only ships the committed `default` profile.

## Provenance

The system persists enough data to prove which config produced which artifacts and outputs.

Current provenance includes:

- case records:
  - `pipeline_profile_name`
  - `pipeline_config_json`
  - `pipeline_config_hash`
  - `index_config_hash`
- case profiles:
  - `pipeline_profile_name`
  - artifact-relevant `index_config_json`
  - artifact-relevant `index_config_hash`
  - extraction-stage model config through the case-profile artifact lineage
- raw PDF chunks:
  - `index_config_hash`
  - `embedding_model`
- historical client packages:
  - `pipeline_profile_name`
  - artifact-relevant `index_config_json`
  - artifact-relevant `index_config_hash`
- product-truth records:
  - `pipeline_profile_name`
  - artifact-relevant `index_config_json`
  - artifact-relevant `index_config_hash`
- retrieval runs:
  - profile name
  - full resolved config
  - `pipeline_config_hash`
  - `index_config_hash`
  - `runtime_config_hash`
- answer API responses:
  - retrieval-run link
  - `pipeline_profile_name`
  - `pipeline_config_hash`
- answer-planning model invocations:
  - packing algorithm/version metadata
  - configured packing order and token budget
  - packed input/output item lineage and packed-evidence hash
- bulk-fill requests:
  - pinned resolved config in `config_json`
  - `pipeline_profile_name`
  - `pipeline_config_hash`
  - `index_config_hash`

## External control surface

This is the control surface an external harness can use today without patching repo code.

### 1. Discover the resolved default profile

- `GET /api/pipeline-config/default`
- returns:
  - full resolved config JSON
  - JSON schema
  - `config_hash`
  - `index_config_hash`
  - `runtime_config_hash`
  - artifact-specific compatibility hashes

Use this endpoint to:

- discover the current committed default
- validate override payloads against the live schema
- compare request-time hashes against the base profile

### 2. Request-time control surface

These are the runtime sweep entry points that exist today.

#### Draft or revise one row

- `POST /api/cases/{case_id}/rows/{row_id}/draft`
- optional JSON fields:
  - `pipeline_profile`
  - `pipeline_override`
  - `reproducibility_mode`
  - `revision_mode_override`

This is the primary request-time sweep path.

What it can vary safely:

- `retrieval.*`
- `packing.*`
- `models.answer_planning.*`
- `models.answer_rendering.*`
- `generation.*`
- `revision.*`

Expected hash behavior:

- `pipeline_config_hash` changes when the resolved request config changes
- `runtime_config_hash` changes when runtime knobs change
- `index_config_hash` stays the same when the override is runtime-only and artifacts remain compatible

#### Launch bulk-fill

- `POST /api/cases/{case_id}/bulk-fill`
- optional JSON fields:
  - `pipeline_profile`
  - `pipeline_override`
  - `reproducibility_mode`

Bulk-fill pins the resolved config into the persisted request.

Operationally this means:

- the worker does not resolve a fresh config later
- a sweep harness can queue multiple bulk-fill requests under different runtime configs on different cases
- the request records both `pipeline_config_hash` and `index_config_hash`

#### Create a new case under explicit index/runtime settings

- `POST /api/cases`
- optional multipart form fields:
  - `pipeline_profile`
  - `pipeline_override` as a JSON object string
  - `reproducibility_mode`

This is the cleanest API entry point for index-time sweeps on fresh live cases.

What it can vary:

- any valid `pipeline_override`, including index-time knobs

Expected behavior:

- the case persists the full resolved config
- current-PDF artifacts and case-profile artifacts are built under that config immediately
- artifact-specific compatibility hashes are derived from the selected indexing config

### 3. Rebuild and import control surface

These are the explicit rebuild/reimport entry points for index-time sweeps on existing artifacts.

#### Historical corpus import

- `python -m app.cli import-historical-corpus --pipeline-profile default --pipeline-config-path path/to/config.json`

#### Product truth import or replace

- `python -m app.cli import-product-truth --pipeline-profile default --pipeline-config-path path/to/config.json`
- `python -m app.cli reimport-product-truth --pipeline-profile default --pipeline-config-path path/to/config.json`

#### Live case rebuild

- `python -m app.cli rebuild-case-index-artifacts <case-id> --pipeline-profile default --pipeline-config-path path/to/config.json`

#### Run-manifest export

- `python -m app.cli export-run-manifest <run-id> --output-path path/to/manifest.json`

Important process-level prerequisite:

- CLI flows read repo-root `.env` and process env, not the already-running backend process state
- if the CLI process does not have `LLM_API_KEY` or legacy `OPENAI_API_KEY`, embedding-backed and LLM-backed rebuild/import commands will fail even if the running backend instance was started with a key

### 4. What is sweepable today

#### Safe runtime sweeps

Use request-time overrides on draft and bulk-fill for:

- retrieval weights
- retrieval scoring weights
- retrieval candidate-pool limits
- retrieval final quotas
- retrieval sufficiency threshold
- retrieval broadening on/off
- dedup mode
- packing token budget and block order
- answer-planning model selection
- answer-rendering model selection
- generation temperature
- target answer length

These do not require artifact rebuilds when the selected index remains compatible.

#### Explicit index-time sweeps

Use case creation or explicit rebuild/reimport flows for:

- embedding model
- current-PDF chunking mode
- current-PDF chunk size and overlap
- current-PDF contextualization
- historical signature mode
- case-profile extraction stage model

These require explicit rebuild/reimport of the affected artifact families.

### 5. Hash semantics for sweep tooling

External tooling should treat these hashes differently.

- `config_hash`
  - hash of the full resolved pipeline config
- `runtime_config_hash`
  - hash of the runtime subset only
- `index_config_hash`
  - hash of the full indexing subset
- artifact-specific compatibility hashes
  - finer-grained hashes used by compatibility checks

Current implementation detail that matters for sweeps:

- live case records persist the full `index_config_hash`
- artifact rows persist artifact-relevant compatibility hashes
- runtime sweeps should hold the request `index_config_hash` constant
- index-time sweeps should expect new artifact-relevant hashes and usually new builds/imports

### 6. What is not externally controllable today

The repo does not currently expose all operations through the same interface.

- only one named profile exists: `default`
- there is no profile catalog API beyond the resolved default endpoint
- export does not accept `pipeline_override`
- `POST /api/product-truth/import` accepts `reproducibility_mode`, but not per-request pipeline overrides
- historical import and live-case rebuild are CLI-only
- the repo does not implement sweep orchestration, benchmark scoring, split management, or tuning loops

### 7. Fail-closed behavior

External sweep tooling should expect explicit failures, not silent fallback, when:

- unknown config fields are supplied
- unsupported non-default values are supplied
- runtime overrides are incompatible with persisted index artifacts
- required rebuilds or reimports have not been run
- `strict_eval` lineage requirements are missing

## Minimal sweep examples

### Runtime sweep on one draft request

Example:

```bash
curl -X POST \
  http://127.0.0.1:8000/api/cases/<case-id>/rows/<row-id>/draft \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-Slug: local-workspace' \
  -H 'X-User-Email: local.user@example.test' \
  -d '{
    "message": "Regenerate the answer with the latest grounded evidence.",
    "thread_id": "<thread-id>",
    "revision_mode_override": "content_change",
    "reproducibility_mode": "best_effort",
    "pipeline_override": {
      "retrieval": {
        "final_quota": {
          "product_truth": 1,
          "historical_exemplars": 1
        }
      },
      "generation": {
        "target_answer_words_max": 80
      }
    }
  }'
```

Expected outcome:

- the request resolves successfully without rebuilding artifacts
- the returned `pipeline_config_hash` changes from the case default
- the returned `index_config_hash` stays equal to the case/index baseline

### Index-time sweep on a fresh case

Example:

```bash
curl -X POST \
  http://127.0.0.1:8000/api/cases \
  -H 'X-Tenant-Slug: local-workspace' \
  -H 'X-User-Email: local.user@example.test' \
  -F 'name=sweep-probe' \
  -F 'client_name=Sweep Probe' \
  -F 'pipeline_override={"indexing":{"current_pdf":{"contextualize_chunks":true}}}' \
  -F 'pdf=@./seed_data/historical_customers/nordtransit_logistik_ag/nordtransit_logistik_ag_context_brief.pdf;type=application/pdf' \
  -F 'questionnaire=@./path/to/questionnaire.xlsx;type=application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
```

Expected outcome:

- the created case persists the selected index-time override
- the case gets a new full `index_config_hash`
- current-PDF chunks are rebuilt under the selected current-PDF artifact hash
- contextualized current-PDF chunks store the deterministic metadata header in `content`

### Index-time sweep on an existing case

Example:

```bash
python -m app.cli rebuild-case-index-artifacts <case-id> \
  --pipeline-profile default \
  --pipeline-config-path ./override.json
```

Use this path when:

- you want to change index-time knobs for an existing live case
- you do not want to create a brand-new case for each sweep point

## Quality profile example

The repo still ships only the committed `default` profile, but a proposal-quality override can be supplied externally.

Documented example override payload, named `proposal_quality_v1`:

```json
{
  "models": {
    "case_profile_extraction": {
      "model_id": "gpt-5.2",
      "reasoning_effort": "medium"
    },
    "answer_planning": {
      "model_id": "gpt-5.2",
      "reasoning_effort": "medium"
    },
    "answer_rendering": {
      "model_id": "gpt-5.2",
      "reasoning_effort": "low"
    }
  },
  "generation": {
    "target_answer_words_min": 70,
    "target_answer_words_max": 150
  }
}
```

This is a documented override example, not a second committed built-in profile. Default behavior remains unchanged unless an explicit override is supplied.

### Import/rebuild flows

Historical and product-truth imports accept explicit config inputs:

- `python -m app.cli import-historical-corpus --pipeline-profile default --pipeline-config-path path/to/config.json`
- `python -m app.cli import-product-truth --pipeline-profile default --pipeline-config-path path/to/config.json`
- `python -m app.cli reimport-product-truth --pipeline-profile default --pipeline-config-path path/to/config.json`
- `python -m app.cli rebuild-case-index-artifacts <case-id> --pipeline-profile default --pipeline-config-path path/to/config.json`

## Rebuild requirements

If you change any index-time config, you must rebuild or reimport the affected artifacts explicitly.

Examples:

- changing `indexing.embedding_model` requires regenerating case PDF chunks, live case profiles, historical case signatures, historical row embeddings, and product-truth embeddings
- changing `indexing.historical.signature_mode` requires reimporting historical packages/case signatures
- changing current-PDF chunking settings requires rebuilding the live case index artifacts
- re-running `import-historical-corpus` is the explicit historical reimport path
- `reimport-product-truth` is the explicit replace/reimport path for product truth

The repo will not silently rebuild these during draft, review, export, or bulk-fill requests.

## Reproducibility mode

Pipeline-facing flows may now run in:

- `best_effort`
- `strict_eval`

`best_effort` preserves default product behavior while recording additive lineage.
`strict_eval` fails loudly when required reproducibility records such as repo/runtime snapshots, manifests, build IDs, prompt hashes, model-invocation lineage, and replay state are missing.

Evaluation safety note:

- additive product-truth import is not allowed in `strict_eval` once approved product-truth records already exist
- use `reimport-product-truth` for clean evaluation baselines
- live requests will not silently rebuild incompatible artifacts; strict-eval operators must reimport or rebuild explicitly before benchmarking
