# Reproducibility Architecture

## Purpose

This document defines the advanced reproducibility model for `rfx-rag-expert`.

It distinguishes three different ideas that must not be conflated:

### Audit lineage

Audit lineage means the repo can prove what happened after the fact.

Examples:
- which inputs were used
- which evidence was selected
- which prompt/request payload was sent
- which output text was returned
- which config/build lineage was referenced

Audit lineage does not imply exact replay.

### Operational replayability

Operational replayability means the repo can rerun the workflow from recorded state if the same external services, models, and compatible runtime environment still exist.

This is the ceiling for external-model stages such as:
- case-profile extraction
- query embeddings
- answer generation

Operational replayability does not imply bitwise determinism.

### Deterministic non-LLM rebuilds

Deterministic non-LLM rebuilds mean the repo can recreate derived non-LLM artifacts from stored inputs, locked runtime identity, code identity, and recorded config/build lineage.

This is the target for:
- source manifests
- PDF extraction and chunking
- build compatibility checks
- retrieval trace reconstruction after embeddings already exist
- export transforms

## Reproducibility levels

The repo records an explicit `reproducibility_level` on each `execution_run`.

- `audit_complete`
  - enough lineage to audit the run after the fact
  - not necessarily replayable
- `operationally_replayable`
  - enough lineage to rerun the workflow if the same provider/runtime remains available
  - used for LLM-backed and embedding-backed stages in `strict_eval`
- `deterministic_non_llm`
  - no live provider call is required for the run itself
  - used for `export` in `strict_eval`
- `strict_eval_eligible`
  - reserved vocabulary for future use
  - not currently emitted by the code

Current label policy:
- `retrieval` in `strict_eval` is `operationally_replayable`, not `deterministic_non_llm`, because query embeddings are still external model calls.
- `export` in `strict_eval` is `deterministic_non_llm` because it is a pure workbook transform once the selected answer versions already exist.

## Reproducibility modes

The repo exposes two modes:

- `best_effort`
- `strict_eval`

`best_effort` preserves current product ergonomics and records additive lineage.

`strict_eval` is for controlled external benchmarking and fails loudly when required lineage is missing.

For benchmark isolation, operators should treat `RFX_DATABASE_URL` and `RFX_STORAGE_ROOT` as a paired baseline.
If you clone a benchmark database into a throwaway database for one sweep run, use a distinct storage root for that run as well whenever the workflow may create or read filesystem-backed objects such as rebuilt artifacts or exports.

## Strict-Eval Guarantees

When a run is started in `strict_eval`, the repo requires:

- repo snapshot capture
- runtime snapshot capture
- backend lock hash capture
- alembic head capture
- required source manifests for import/create/rebuild/export runs
- required build IDs for live-case retrieval paths
- model-invocation lineage for:
  - case-profile extraction
  - query embeddings
  - answer planning
  - answer rendering
- retrieval replay state including:
  - candidate pools
  - selected evidence
  - request date
  - current build IDs
  - query-embedding lineage
- export input manifests with row selection
- export answer-version lineage for every exported row

`strict_eval` does not silently downgrade to `best_effort`.

## What Strict-Eval Does Not Guarantee

`strict_eval` still does not mean:

- bitwise-identical provider outputs
- immunity to provider-side model alias drift
- exact replay if the external provider no longer supports the recorded model behavior
- byte-identical outputs across different locked runtimes

It is a fail-closed control mode, not a claim of model determinism.

## Product-Truth Evaluation Safety

Product truth remains canonical and separately versioned.

For evaluation use:
- additive `import-product-truth` is not allowed in `strict_eval` once approved product-truth records already exist
- use `reimport-product-truth` to create a clean canonical corpus baseline
- `POST /api/product-truth/import` also accepts `reproducibility_mode`; in `strict_eval` it follows the same additive-import prohibition

This prevents evaluation runs from silently using duplicate active canonical truth.

## Legacy Artifacts

Legacy artifacts created before the reproducibility layer may still exist in development databases.

Policy:
- `best_effort` may continue using them where older product behavior allowed it
- `strict_eval` rejects them when required build or lineage fields are missing
- request handling does not silently backfill missing lineage

Required operator action is explicit rebuild or reimport.

## Advanced Benchmarking Workflow

Before using the repo as an external benchmarking baseline:

1. Ensure the worktree is clean or intentionally accept a captured dirty snapshot for a non-frozen run.
2. Apply all migrations: `python -m alembic -c backend/alembic.ini upgrade head`.
3. Reimport the historical sample corpus in `strict_eval`.
4. Reimport product truth in `strict_eval`.
5. Rebuild any live benchmark cases in `strict_eval`.
6. Run the benchmark drafts/exports in `strict_eval`.
7. Export canonical run manifests for the reference runs.

Recommended commands:

```bash
python3 -m alembic -c backend/alembic.ini upgrade head
python3 -m app.cli import-historical-corpus --reproducibility-mode strict_eval
python3 -m app.cli reimport-product-truth --reproducibility-mode strict_eval
python3 -m app.cli rebuild-case-index-artifacts <case-id> --reproducibility-mode strict_eval
python3 -m app.cli export-run-manifest <run-id> --output-path ./reference-manifests/<name>.json
```

Recommended benchmark-clone pattern:

- keep one frozen baseline database and storage root
- clone the baseline database into a throwaway benchmark database per knob set
- point `RFX_DATABASE_URL` at that throwaway database for the run
- point `RFX_STORAGE_ROOT` at a matching throwaway storage directory for the run when artifacts or exports may be touched
- drop the throwaway database and remove the throwaway storage directory after the run

On Win11, use the same commands from an activated venv or the helper flow in [windows-local-setup.md](windows-local-setup.md).

## Reference Run Fixtures

The repo does not implement benchmark logic, but it does support canonical fixture generation.

Minimal process:
- create or rebuild the sample baseline under `strict_eval`
- run one or more known draft/export flows
- export the resulting `execution_run` manifests
- treat those manifest JSON files as the reference interoperability fixtures for external tooling

The exported run manifest includes the captured runtime env fingerprint, including the resolved `storage_root`, so external tooling can distinguish runs that executed against different storage baselines.

Prefer exporting these manifests outside the tracked repo tree so the repository baseline stays clean.

This is a manifest-generation workflow, not an evaluator.

## Design policy

- External LLM calls are not advertised as bitwise deterministic unless the repo can prove that.
- Non-LLM artifact rebuilds should be deterministic from stored state and locked runtime identity where possible.
- Imports, rebuilds, retrieval runs, draft/revision runs, bulk-fill executions, and exports are persisted as explicit execution runs.
- Mutable active pointers remain for product behavior, but immutable run/build history is the reproducibility source of truth.
