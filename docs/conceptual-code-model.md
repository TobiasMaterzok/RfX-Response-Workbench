# Conceptual Code Model

This document describes the actual conceptual model implemented in `rfx-rag-expert`, based on the docs plus a code-level trace through models, services, routers, tests, and frontend state.

It is not just a product overview. It is the working mental model the code assumes.

## 1. Core Thesis

The repo is a **case-scoped RfX drafting system** with five coupled layers:

1. **Business objects**
   - tenant, user, case, questionnaire, row, answer, export, bulk-fill job
2. **Evidence corpora**
   - live case facts, raw current PDF chunks, product truth, historical exemplars
3. **Generation lineage**
   - retrieval runs, snapshot items, answer versions, evidence links, chat messages
4. **Pipeline lineage**
   - resolved pipeline config, artifact index hashes, compatibility checks
5. **Reproducibility lineage**
   - source manifests, execution runs, artifact builds, repo/runtime snapshots, model invocations

The application is therefore not “generate answer from question”. It is:

`scoped source artifacts -> indexed evidence corpora -> retrieval snapshot -> normalized/packed evidence -> staged plan/render generation -> explicit review state -> export`

## 2. The Real Bounded Contexts

The code splits into these conceptual contexts:

- **Identity and scope**
  - tenant, user, membership, request headers
- **Live case ingestion**
  - uploads, PDF extraction, case profile creation, questionnaire ingestion
- **Historical exemplar corpus**
  - dataset, workbook, paired PDF package, historical case profile, historical QA rows
- **Canonical product truth**
  - approved vendor truth records and retrievable chunks
- **Retrieval**
  - retrieval request, query embeddings, candidate pools, scored evidence, snapshot persistence
- **Answer generation**
  - chat thread, revision classification, AnswerPlan, rendering, answer version persistence
- **Review and export**
  - explicit approval pointer per row, deterministic export selection, aligned XLSX/CSV/ZIP
- **Bulk fill**
  - durable request queue, row executions, lifecycle events, worker claim model
- **Reproducibility**
  - execution runs, artifact builds, source manifests, model invocations, strict-eval checks

## 3. Top-Level Architecture

The implemented storage model is:

- **Relational database**
  - almost all application state, retrieval snapshots, lineage, job state
- **PostgreSQL + pgvector or JSON fallback**
  - embeddings on semantic corpora and retrieval features
- **Object storage**
  - local filesystem via `LocalObjectStorage`, but uploads are also duplicated in DB `payload`

Three important consequences follow:

- the repo persists **normalized workflow state**, not just final answers
- the repo persists **semantic artifacts as first-class objects**
- the repo persists **reproducibility metadata as first-class objects**

Schema bootstrapping detail:

- the public baseline migration is a **PostgreSQL-only frozen schema snapshot**, not a hand-written incremental migration chain
- the baseline lives in [backend/alembic/versions/20260310_0001_public_release_baseline.py](../backend/alembic/versions/20260310_0001_public_release_baseline.py)
- this reinforces the repo assumption that first public setup starts from a fresh PostgreSQL database

## 4. Entity Model By Context

### 4.1 Identity and Scope

Defined in [backend/app/models/entities.py](../backend/app/models/entities.py#L59).

- `User`
  - attributes: `email`, `display_name`
  - relationship: has many `Membership`
- `Tenant`
  - attributes: `name`, `slug`
  - relationship: has many `Membership`
- `Membership`
  - attributes: `tenant_id`, `user_id`, `role`
  - invariant: unique `(tenant_id, user_id)`

Scope is enforced from headers `X-Tenant-Slug` and `X-User-Email` in [backend/app/api/deps.py](../backend/app/api/deps.py) and resolved by [backend/app/services/identity.py](../backend/app/services/identity.py).

### 4.2 Live Case Domain

Core case entities begin at [backend/app/models/entities.py](../backend/app/models/entities.py#L91).

- `RfxCase`
  - business identity: `name`, `client_name`, `language`, `status`
  - pipeline identity: `pipeline_profile_name`, `pipeline_config_json`, `pipeline_config_hash`, `index_config_hash`
  - reproducibility pointers: `creation_run_id`, `current_pdf_build_id`, `case_profile_build_id`
  - relationships: `uploads`, `questionnaires`, `case_profiles`, `chats`
- `Upload`
  - logical file registry for case PDF, questionnaire, exports, and historical workbooks
  - provenance: `original_file_name`, `media_type`, `object_key`, `file_hash`, `size_bytes`, optional raw `payload`

Product-truth import currently records source provenance through `SourceManifest`, not an `Upload` row.

Creation flow is implemented in [backend/app/services/cases.py](../backend/app/services/cases.py#L84).

### 4.3 Live Case PDF Evidence

Live PDF evidence is split into two layers:

- `PdfPage`
  - exact extracted page text plus `text_hash`
- `PdfChunk`
  - retrieval-ready segments with `page_number`, `chunk_index`, `start_offset`, `end_offset`, `chunking_version`, `embedding_model`, `index_config_hash`, `artifact_build_id`, `chunk_hash`, `content`, `embedding`

Chunking behavior lives in [backend/app/services/pdf_chunks.py](../backend/app/services/pdf_chunks.py). The important concepts are:

- chunking is deterministic
- legacy default is character-based
- token chunking is optional via pipeline config
- contextualized chunk text changes stored content and hash

### 4.4 Live Case Structured Profile

Structured extraction is modeled twice:

- `CaseProfileDocument`
  - full wrapped document schema in [backend/app/schemas/case_profile.py](../backend/app/schemas/case_profile.py#L30)
  - includes `support_level` and `unknowns`
- persisted relational form:
  - `CaseProfile`
  - `CaseProfileItem`

Important nuance:

- the full `CaseProfile.document` JSON keeps the complete extraction ledger
- flattened `CaseProfileItem` rows only persist `prompt`, `answer`, `confidence`, `citations`, `normalized_text`, `embedding`
- `support_level` and `unknowns` are **not** exploded into searchable columns

This means the canonical extraction artifact is the JSON document, while retrieval indexes a reduced searchable projection.

Persistence is implemented in [backend/app/services/case_profiles.py](../backend/app/services/case_profiles.py#L168).

### 4.5 Questionnaire Domain

Questionnaire state begins at [backend/app/models/entities.py](../backend/app/models/entities.py#L583).

- `Questionnaire`
  - one questionnaire per case in current schema
  - provenance: `upload_id`, `source_file_name`, `source_sheet_name`, `file_hash`, `schema_version`
- `QuestionnaireRow`
  - stable row identity: `source_row_id`
  - source position: `source_row_number`, `source_sheet_name`
  - row content: `context_raw`, `question_raw`, `answer_raw`, `normalized_text`
  - review state: `review_status`, `approved_answer_version_id`, `last_error_detail`

Important invariants:

- workbook headers are exact: `Context`, `Question`, `Answer`
- row ids are deterministic `file:sheet:row_number`
- case questionnaires allow empty answer cells, historical workbooks do not

Workbook parsing lives in [backend/app/services/workbooks.py](../backend/app/services/workbooks.py).

### 4.6 Historical Exemplar Corpus

Historical corpus is normalized into five objects:

- `HistoricalDataset`
  - dataset root, schema version, approval state, active artifact build
- `HistoricalWorkbook`
  - one workbook per client package
- `HistoricalClientPackage`
  - paired PDF package metadata and index lineage
- `HistoricalCaseProfile`
  - structured profile derived from the paired PDF
  - stores `signature_text` and `signature_embedding`
- `HistoricalQARow`
  - normalized historical row
  - key retrieval text is `context_raw + question_raw`
  - `answer_raw` is preserved for generation but not primary recall

Important invariants:

- every approved historical row must link to a workbook, client package, and historical case profile
- live-to-historical case similarity uses `HistoricalCaseProfile.signature_embedding`
- same-source leakage is excluded by row id, workbook file, and paired PDF provenance

Import flow lives in [backend/app/services/seed.py](../backend/app/services/seed.py#L52).

### 4.7 Product Truth Corpus

Product truth is a separate canonical corpus, not exemplar data.

- `ProductTruthRecord`
  - canonical vendor truth record with `product_area`, `title`, `body`, `language`, source provenance, effective date range, version, approval status, index lineage, artifact build
- `ProductTruthChunk`
  - retrievable chunk linked to a truth record

Important nuance:

- current implementation creates **one chunk per truth record**
- the model still treats `ProductTruthChunk` as the retrieval unit

Ingestion lives in [backend/app/services/product_truth.py](../backend/app/services/product_truth.py#L199).

### 4.8 Conversation, Retrieval, and Answer State

This is the operational heart of the app.

- `ChatThread`
  - one conversational branch for one case row
  - not globally one-per-row; multiple threads per row are allowed
- `ChatMessage`
  - ordered user/assistant/system messages inside a thread
  - may point to `answer_version_id` and `retrieval_run_id`
- `RetrievalRun`
  - persisted retrieval snapshot root
  - stores `query_text`, `request_context`, and `prompt_authority_order`
- `RetrievalSnapshotItem`
  - ranked selected evidence item with source identity, excerpt, metadata, score
- `AnswerVersion`
  - immutable answer revision for one row/thread
  - points to one `retrieval_run` and one rendering `model_invocation`
- `EvidenceLink`
  - explicit join from answer version to retrieval snapshot items

Important invariants:

- an answer version is always tied to exactly one retrieval snapshot
- approval is not stored on `AnswerVersion`; it is an explicit pointer on `QuestionnaireRow`
- the linked retrieval snapshot is part of the persisted answer contract

Important nuance:

- a `ChatThread` is not equivalent to an answer
- a thread can contain:
  - one or more user messages
  - zero or more retrieval runs
  - zero or more answer versions
- the code intentionally persists thread and retrieval lineage before answer persistence is guaranteed
- therefore a failed draft or bulk-fill attempt can leave a thread with messages and retrieval evidence but no `AnswerVersion`

This leads to four row-attempt states that are conceptually separate from row review state:

- `none`
  - no persisted thread/attempt for the row
- `pending_no_answer`
  - thread/attempt exists but no answer version exists and no failure has been finalized
- `failed_no_answer`
  - thread/attempt exists, no answer version exists, and execution lineage finalized as failed
- `answer_available`
  - the latest inspected thread has at least one persisted answer version

The key boundary is:

- **attempt state** answers “what happened in the latest drafting attempt?”
- **answer history** answers “which persisted answer versions exist for this row?”
- **review state** answers “what does export/review currently consider approved/rejected/etc?”

Those three must not be collapsed into one UI or API concept.

### 4.9 Review, Export, and Bulk Fill

Review/export model:

- `ExportJob`
  - export mode, source upload, output upload, row mapping, metadata, execution run

Bulk-fill model:

- `BulkFillRequest`
  - durable job root with queue/claim lifecycle, parent request pointer, resolved pipeline config, summary state
- `BulkFillRowExecution`
  - per-row attempt record with status, diagnostics, answer version, execution run
- `BulkFillJobEvent`
  - append-only lifecycle/event log

Important nuance:

- `BulkFillRowExecution.attempt_number` increments per row across all requests, not just inside one job
- bulk fill always drafts with `thread=None`, so it creates a fresh thread per row attempt
- `BulkFillRowExecution` and related `ExecutionRun` records describe operational attempt outcome, not the row's approved content state
- a failed bulk-fill attempt may therefore create:
  - a fresh thread
  - one or more user messages
  - a retrieval run and snapshot
  - no answer version
- this is expected lineage, not corrupt data

Approved-row protection is an explicit invariant:

- if a row already has `approved_answer_version_id`, bulk-fill should not overwrite it
- request creation excludes already-approved rows from the target set
- worker execution re-checks approval and skips a row that became approved after queueing
- approval remains a pointer on `QuestionnaireRow`, so operational attempt history never silently replaces the approved answer

## 5. Pipeline Model

Pipeline config is defined in [backend/app/pipeline/config.py](../backend/app/pipeline/config.py#L216) and resolved by [backend/app/pipeline/config.py](../backend/app/pipeline/config.py#L378).

Conceptually the repo distinguishes:

- **index-time config**
  - embedding model
  - current PDF chunking
  - historical signature mode
  - case-profile extraction stage model
- **runtime config**
  - retrieval weights, quotas, broadening, dedup
  - packing limits/order
  - planning/rendering model config
  - generation targets
  - revision policy

The code persists:

- full resolved config hash
- index config hash
- runtime config hash
- artifact-specific compatibility hashes

Compatibility checking happens in [backend/app/pipeline/config.py](../backend/app/pipeline/config.py#L550).

## 6. Retrieval Model

Retrieval request construction starts in [backend/app/services/retrieval.py](../backend/app/services/retrieval.py#L547).

The retrieval feature set is:

- row question
- row context
- row question + context
- revision intent for content-changing revisions
- live case signature text

The corpora are:

- `current_case_facts`
  - `CaseProfileItem`
- `raw_current_pdf`
  - `PdfChunk`
- `product_truth`
  - `ProductTruthChunk` joined to `ProductTruthRecord`
- `historical_exemplar`
  - `HistoricalQARow` joined to `HistoricalClientPackage` and `HistoricalCaseProfile`

Candidate generation uses:

- keyword prefilter
- pgvector prefilter when on PostgreSQL
- explicit broadened scope fallback

Ranking is per corpus and combines:

- semantic similarity
- keyword overlap
- revision-intent similarity where relevant
- current-case-signature similarity for product truth and historical case matching
- language bonus/penalty

Selection is **quota-based per corpus**, not a single unrestricted global top-k.

Important nuance:

- `RetrievalRun.prompt_authority_order` currently persists `["current_case_facts", "product_truth", "historical_exemplars"]`
- raw current PDF evidence is still retrieved and snapshotted separately, but is treated as the same factual authority tier as current case facts

Retrieval run persistence is implemented in [backend/app/services/retrieval.py](../backend/app/services/retrieval.py#L1570).

## 7. Answer Generation Model

Answer generation is staged:

1. retrieval snapshot
2. normalized evidence items
3. evidence packing
4. AnswerPlan generation
5. answer rendering
6. deterministic validation
7. answer version persistence

Core schema:

- `AnswerPlan` in [backend/app/schemas/answer_plan.py](../backend/app/schemas/answer_plan.py#L102)

Key idea:

- the plan encodes **claim-level authority**
- product claims must be backed by `product_truth`
- historical exemplars are usually style/pattern only
- thread history is editorial context only and never evidence

Planning prompt and rendering prompt live in:

- [backend/app/prompts/answer_planning.py](../backend/app/prompts/answer_planning.py#L118)
- [backend/app/prompts/answer_rendering.py](../backend/app/prompts/answer_rendering.py#L77)

Validation and evidence normalization live in [backend/app/services/answer_prompting.py](../backend/app/services/answer_prompting.py).

## 8. Revision Model

The repo implements three effective revision modes in [backend/app/services/answers.py](../backend/app/services/answers.py#L403):

- `initial_draft`
- `content_change`
- `style_only`

Behavior:

- `initial_draft`
  - retrieval refresh
  - new AnswerPlan
  - render without prior thread history
- `content_change`
  - retrieval refresh
  - new AnswerPlan
  - render without prior thread history
- `style_only`
  - reuse prior retrieval snapshot
  - reuse prior planning invocation and AnswerPlan
  - render with prior thread history as editorial context

Frontend-triggered variants:

- first draft
  - frontend sends no revision override
  - backend classifies it as `initial_draft`
- regenerate answer
  - frontend explicitly sends `revision_mode_override="content_change"`
  - this is a forced refresh path, not a style pass
- revise answer
  - frontend explicitly sends `revision_mode_override="style_only"`
  - this is a forced render-only reuse path
- retry after a failed attempt with no answer version
  - frontend sends a fresh initial-generation request with `thread_id=null`
  - conceptually this behaves like a new draft attempt, not like a revision of missing output

Critical forensic nuance:

- style-only reuse is implemented in `answers.py` by directly reusing the prior `RetrievalRun` and prior planning invocation
- the retrieval service contains a reusable-snapshot cloning path, but `draft_answer_for_row` does not currently call it
- so a style-only revision produces a **new answer version and execution run**, but usually **not a new retrieval run**

This is one of the most important things to preserve when changing “regenerate answer” behavior.

## 9. Review and Approval Model

Review state belongs to `QuestionnaireRow`, not to the thread and not to the answer history.

- `needs_review`
  - row has draft output needing human decision
- `approved`
  - row points to exactly one approved answer version
- `rejected`
  - row is explicitly rejected
- `failed`, `running`, `skipped`, `not_started`
  - operational states

Approval logic:

- approving an answer marks that version `accepted`
- row stores `approved_answer_version_id`
- export in `approved_only` uses that explicit pointer

## 10. Export Model

Export flow is implemented in [backend/app/services/exports.py](../backend/app/services/exports.py#L240).

Selection semantics:

- `approved_only`
  - export approved answer or deterministic placeholder
- `latest_available`
  - export latest answer version or deterministic placeholder

Important invariant:

- XLSX and CSV are rendered from the same resolved row-selection pass
- ZIP is the primary bundled artifact

## 11. Bulk-Fill Model

Bulk fill is not an implicit loop. It is a persisted job system in [backend/app/services/bulk_fill.py](../backend/app/services/bulk_fill.py#L239) and [backend/app/services/bulk_fill.py](../backend/app/services/bulk_fill.py#L975).

Lifecycle:

1. create `BulkFillRequest`
2. create one `BulkFillRowExecution` per eligible row
3. worker claims job with `claim_id`, `runner_id`, `heartbeat`
4. execute rows one at a time
5. create normal answer versions through `draft_answer_for_row`
6. leave rows in `needs_review`
7. finish, cancel, fail, or orphan request

Important nuances:

- approved rows are excluded at request creation time
- rows approved after queueing are skipped at execution time
- rejected rows are eligible for regeneration
- bulk fill disables thread-history replay during rendering

## 12. Reproducibility Model

Reproducibility starts at [backend/app/services/reproducibility.py](../backend/app/services/reproducibility.py#L774).

Objects:

- `RepoSnapshot`
  - git commit, dirty state, diff hash/text
- `RuntimeSnapshot`
  - Python/runtime/package/db fingerprint
- `SourceManifest`
  - canonical description of source inputs
- `ExecutionRun`
  - one persisted run of a meaningful operation
- `ArtifactBuild`
  - immutable build lineage for semantic artifacts
- `ModelInvocation`
  - persisted request/response lineage for extraction, planning, rendering, embeddings

Run types include:

- historical import/reimport
- product-truth import/reimport
- live case create/rebuild
- retrieval
- row draft/revision
- bulk-fill job
- bulk-fill row attempt
- export

Strict-eval consistency checks are enforced in [backend/app/services/reproducibility.py](../backend/app/services/reproducibility.py#L950).

## 13. Frontend Mental Model

The frontend is a thin projection of backend state:

- `CaseDetail`
  - selected case summary + case profile + row list + bulk-fill summary/history + threads
- `QuestionnaireRow`
  - combines review state, latest attempt state, and last bulk-fill attempt info
- `ThreadDetail`
  - selected conversational branch with answer version, retrieval summary, and evidence
- `AnswerVersion`
  - history list for the row

Types live in [frontend/src/types.ts](../frontend/src/types.ts), API bindings in [frontend/src/lib/api.ts](../frontend/src/lib/api.ts), and workspace state orchestration in [frontend/src/App.tsx](../frontend/src/App.tsx#L125).

Important UI semantics:

- “Revise answer” is explicitly `style_only`
- “Regenerate answer” is explicitly `content_change`
- “Retry answer” after `failed_no_answer` is a fresh generation request, not a reuse path
- answer history, approved answer, and evidence are all shown separately
- developer panel exposes render-stage prompt capture only, not planning-stage lineage

Another frontend nuance:

- `latest_attempt_state` is partly backend-projected and partly client-derived
- the workspace treats any currently selected answer version as `answer_available` even if the row summary still reflects an older attempt state

## 14. Non-Obvious or Easy-To-Miss Facts

- The row-drafting orchestration lives in `draft_answer_for_row()`, while `AIService` now exposes the staged `plan_answer()` + `render_answer()` surface.
- Live case signature text is recomputed at retrieval time from the current case profile; it is not stored as a separate live table row.
- Historical case signatures are persisted as part of `HistoricalCaseProfile`.
- `QuestionnaireRow.normalized_text` exists, but questionnaire rows are not currently used as a retrieval corpus.
- `EvidenceSourceKind.QUESTIONNAIRE_ROW` exists as an enum value, but is not part of the active retrieval path.
- Answer versions store render-stage request/response text directly; planning-stage payloads live in `ModelInvocation`.
- Product truth and historical exemplar evidence are kept separate all the way through retrieval snapshot items and frontend grouping.

## 15. What “Regenerate Answer” Must Respect

If we change regenerate logic later, these are the conceptual rules the implementation must preserve:

- regeneration is a **new answer version**, never an in-place overwrite
- regeneration must preserve row-level approval semantics
- content-changing regeneration must refresh retrieval and produce a new AnswerPlan
- style-only revision must keep reuse semantics unless explicitly redesigned
- retrieval snapshot and evidence links remain part of the answer contract
- bulk-fill behavior must stay aligned with the same grounded row-drafting path
- pipeline compatibility and reproducibility lineage must remain intact

## 16. Second-Pass Confirmations

A second pass over the API/frontend/test surface confirmed these additional implementation facts:

- the API response contract intentionally projects both row review state and last bulk-fill/attempt state onto each `QuestionnaireRowResponse`
- the pipeline API currently exposes only the resolved committed default profile, not a profile catalog
- sample data docs, seed import, and retrieval tests all consistently treat the shipped historical workbooks as **historical exemplars only**, never canonical product truth
- end-to-end and frontend tests explicitly lock in the staged generation paths:
  - `two_stage_plan_render`
  - `render_only_reuse_plan`
