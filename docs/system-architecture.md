# System architecture

## Goal
Build a case-scoped RfX AI application that can ingest one client PDF and one questionnaire workbook, extract structured case facts, retrieve from approved historical exemplars and product truth, draft grounded answers in chat, and export a filled workbook.

## Preferred runtime architecture
Use a normalized architecture with three storage layers:
- **Relational app database** for users, tenants, cases, uploads, workbook rows, chats, answer versions, provenance, and processing runs.
- **Vector search layer** for semantic retrieval over historical Q&A, case-profile items, and product-truth chunks.
- **Object storage** for original PDFs, XLSX files, and exported workbooks.

For the first implementation, prefer **PostgreSQL + pgvector** for the relational and vector layers unless an explicit task changes that choice.

## Core bounded contexts
- `backend/app/api/`: HTTP endpoints, request validation, and scoped API responses.
- `backend/app/pipeline/`: committed pipeline profile, schema validation, hashing, and compatibility checks.
- `backend/app/services/`: case creation, ingestion, retrieval, generation, export, worker, and reproducibility orchestration.
- `backend/app/models/`: ORM entities and enums for the persisted contract.
- `backend/alembic/versions/`: the public baseline schema migration for fresh PostgreSQL installs.
- `frontend/src/`: case workspace, row drafting UI, evidence view, and optional developer-only panels.

## Retrieval design
Do not use one giant denormalized table with columns like `question_1_pdf ... question_N_pdf`.

Instead, keep separate normalized corpora:
- `historical_qa_rows`
- `historical_client_packages`
- `historical_case_profiles`
- `historical_case_profile_items`
- `case_profile_items`
- `product_truth_chunks`
- `pdf_pages` for extraction traceability
- `pdf_chunks` as the persisted raw current-PDF retrieval corpus

Retrieval should be **hybrid**:
- embedding similarity,
- keyword/full-text search,
- metadata filters,
- optional reranking.

Historical exemplars must remain clearly labeled as historical. Product truth must remain clearly labeled as canonical vendor truth.
Historical exemplar recall must combine:
- row intent similarity from `Context + Question`,
- live-to-historical case similarity from versioned case-profile signature text/embeddings,
- explicit language preference and provenance-based leakage exclusion.

Retrieval/generation must run under an explicit resolved pipeline config:
- index-time config and runtime config are distinct,
- artifact compatibility is proven by artifact-relevant compatibility hashes plus the selected `index_config_hash`,
- runtime overrides are allowed only when compatible with persisted artifacts,
- unsupported non-default knobs fail loudly.

Current-case retrieval must keep two separate evidence layers:
- structured `case_profile_items`,
- raw current `pdf_chunks` when the structured profile is not sufficient by itself.

Retrieval execution should also persist:
- candidate-generation stage traces,
- explicit broadened/degraded markers,
- retrieval sufficiency status,
- revision classifier metadata,
- reuse vs refresh reasoning.

## Conversation and answer state
A conversation belongs to exactly one `rfx_case`.
Each workbook row in an active questionnaire has a stable row identifier.
Each row may have multiple `chat_threads`; a thread is a drafting-attempt container, not the approved answer.
Each thread belongs to exactly one case and one row.
Threads may persist `chat_messages` and `retrieval_runs` even when no `answer_version` is produced.
Each saved answer is an `answer_version` tied to exactly one case and one row.
Store retrieval snapshots and evidence links for every generated answer version.
Keep latest attempt state, row review/approval state, and answer-version history as separate concepts.
Only `answer_versions` are reviewable/exportable artifacts; failed or pending no-answer attempts are inspectable but not reviewable.
Bulk-fill jobs and row executions should be normalized persisted records rather than implicit loops with in-memory state.

## PDF analysis design
Every case PDF must produce a versioned `case_profile` using a fixed, versioned prompt set.
Use strict structured outputs and schema validation.
Keep unknowns explicit. Missing evidence must not be guessed.
The model should return only the extraction core; server-owned metadata such as case ID, file hash, and model name should be wrapped around that output by the backend before persistence.
Store both:
- the normalized `case_profile`, and
- raw text chunks with page numbers for traceability and retrieval.

## Prompting architecture

Answer generation is intentionally staged.

### 1. Case-profile extraction

- strict structured extraction only
- fixed analysis dimensions
- support level, confidence, citations, and unknowns per item
- no customer-facing prose
- no model-generated system metadata such as case ID, file hash, or generated timestamp

### 2. Answer planning

- internal-only `AnswerPlan` JSON
- classifies `primary_intent` plus `secondary_intents` for compound questions
- limits factual support to the correct authority layer at claim level
- requires `product_truth` for product, integration, security, and reporting claims
- keeps historical exemplars as style or pattern support by default
- records forbidden claims and material unknowns explicitly in customer-visible wording

### 3. Answer rendering

- customer-facing answer text only
- renders from the validated `AnswerPlan`
- also receives the current revision request
- for interactive `style_only` revisions, may replay prior thread history as editorial context only
- first drafts, content-changing replans, and bulk-fill attempts render without prior thread history
- prior thread history is not evidence and must not override the current `AnswerPlan`
- must not mention internal evidence labels or retrieval mechanics
- is checked by deterministic render-time validation before persistence

This replaces the previous one-shot answer prompt as the primary generation path.

Operational consequence:

- live and historical case-profile artifacts produced under the old extraction prompt/schema contract are semantically stale after this change
- rebuild or reimport must be explicit
- retrieval now fails loudly when case-profile artifacts do not match the current extraction schema/prompt-set contract

Current persisted lineage versions:

- extraction core schema: `rfx_case_profile_extraction.v2`
- wrapped live/historical case-profile artifact schema: `rfx_case_profile.v3`
- wrapped case-profile prompt set: `rfx_case_profile_prompt_set.v3`
- answer planning schema: `rfx_answer_plan.v2`

## Product truth
Historical Q&A is not enough.
The application also needs a separate product-truth corpus for vendor capabilities, deployment models, integrations, security posture, delivery model, and supported operating assumptions.
See `docs/product-truth-contract.md`.

## Bulk-fill and review workflow
Bulk-fill should:
- create a persisted job/request,
- be durably queued and explicitly claimed by a worker path,
- progress rows deterministically,
- persist row-level execution status incrementally,
- persist job/row lifecycle history,
- preserve answer versions, retrieval snapshots, and evidence links,
- allow a row attempt to persist thread/message/retrieval lineage even when generation fails before an `answer_version` is created,
- support explicit retry/resume/cancel behavior,
- leave generated rows in `needs_review` or equivalent rather than auto-approving them.

Approved-row protection:

- bulk-fill must not overwrite rows that already have an approved answer
- approved rows should be excluded when the request is created
- if a row becomes approved after queueing but before execution, the worker must skip it and preserve the approved answer pointer
- `BulkFillRowExecution` and related `execution_runs` record operational attempt outcome, not the row's approved content state

Local/dev should run the same persisted job model through an explicit worker command rather than ad hoc hidden background logic.

Export should support explicit `approved_only` and `latest_available` modes, generate aligned XLSX and CSV artifacts from one resolved row-selection pass, and deliver them as a single ZIP download in the UI.

## Pipeline config

The repo ships one committed default pipeline profile and exposes a typed config surface for external orchestration.

Required properties:
- default behavior is unchanged when no config is supplied,
- config is versioned and strictly validated,
- index-time changes require explicit rebuild/reimport,
- retrieval runs and jobs persist config provenance and hashes.

Compatibility should use artifact-relevant index lineage rather than one overly coarse full-index hash so operators can sweep live case chunking without forcing unnecessary rebuilds of unrelated corpora.

See [pipeline-config.md](pipeline-config.md).

## Reproducibility architecture

The repo now adds a reproducibility layer built around:
- repo snapshots
- runtime snapshots
- source manifests
- execution runs
- artifact builds
- model invocations

The intent is:
- audit-complete lineage for all meaningful runs
- operational replayability for LLM-backed or embedding-backed stages
- deterministic non-LLM rebuilds where possible
- explicit `best_effort` vs `strict_eval` behavior

Current label policy:
- `retrieval` is operationally replayable in `strict_eval` because it still depends on recorded query-embedding model calls
- `export` is deterministic non-LLM in `strict_eval` because it is a pure workbook transform over already-materialized answers

See [reproducibility-architecture.md](reproducibility-architecture.md) for the advanced benchmarking and provenance surface.

## Suggested first implementation path
1. case creation and file registration
2. strict XLSX ingestion and row IDs
3. PDF extraction and `case_profile` generation
4. historical exemplar ingestion into retrieval store
5. single-row chat drafting with evidence panel
6. answer versioning and export
7. bulk workbook fill with review workflow
