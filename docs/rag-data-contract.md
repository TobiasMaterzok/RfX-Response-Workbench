# RAG data contract

## Scope
This document defines the strict data contracts for:
- historical XLSX ingestion,
- PDF case-profile extraction,
- retrieval inputs,
- provenance expectations.

## 1. Historical XLSX row contract
Historical workbooks are expected to provide rows with these semantic columns:
- `Context`
- `Question`
- `Answer`

### Required rules
- Column names are exact. Do not silently accept near matches.
- Each ingested row must get a stable `source_row_id` derived from source workbook, worksheet, and row number.
- Empty `Question` or `Answer` values are invalid unless a future schema version explicitly allows them.
- The source language must be recorded as observed or inferred with an explicit confidence field.
- The raw cell text must be preserved alongside normalized retrieval text.
- The primary historical retrieval text must be `Context + Question`; `Answer` remains preserved for exemplar generation but must not be the primary recall field.
- Every persisted historical row must link to a historical client package that proves the paired PDF and historical case-profile provenance.

### Recommended normalized historical record shape
Use a normalized record rather than a single giant denormalized table.

Example logical shape:
- `record_id`
- `tenant_id`
- `dataset_id`
- `client_name`
- `source_file_name`
- `source_sheet_name`
- `source_row_number`
- `source_row_id`
- `language`
- `approval_status`
- `context_xlsx`
- `question_xlsx`
- `answer_xlsx`
- `file_hash`
- `schema_version`
- `ingested_at`

Embeddings should be stored separately or in a linked table/index, not by duplicating large vector fields into every unrelated table.

### Required historical client-package companion artifacts
Each historical client package must include:
- the paired historical PDF,
- a validated historical `case_profile` generated from that PDF with the same versioned prompt set used for live cases,
- persisted historical case-profile items,
- a historical case summary / signature text,
- a persisted case-signature embedding for case-to-case similarity,
- provenance linking every `historical_qa_row` back to the package, workbook, and paired PDF.

If any companion artifact is missing or mismatched, ingestion or retrieval must fail loudly.

## 2. PDF case-profile contract
Every uploaded or seed PDF must be transformed into a structured `case_profile` document.

### Schema shape
The current implementation uses two related versioned schemas:

- model-facing extraction output: `rfx_case_profile_extraction.v2`
- persisted wrapped document: `rfx_case_profile.v3`

The extraction output contains only the analysis ledger and summary. The backend then wraps that output with server-owned metadata before persistence.

Persisted document shape:

```json
{
  "schema_version": "rfx_case_profile.v3",
  "case_id": "...",
  "source_pdf": {
    "file_name": "...",
    "file_hash": "..."
  },
  "language": "de|en|unknown",
  "client_name": "...",
  "analysis_items": [
    {
      "id": "strategic_objectives",
      "prompt": "What are the customer's strategic objectives, transformation drivers, and success criteria?",
      "answer": "...",
      "support_level": "explicit|strongly_implied|unknown",
      "confidence": "high|medium|low",
      "citations": ["Page 1"],
      "unknowns": []
    }
  ],
  "summary": "...",
  "generated_at": "...",
  "model": "..."
}
```

### Canonical analysis item IDs
The initial implementation should use a fixed, versioned prompt set. Start with these IDs:
1. `strategic_objectives`
2. `initiative_scope`
3. `business_capabilities_in_scope`
4. `geographies_entities_operating_model`
5. `current_state_pain_points`
6. `target_state_outcomes`
7. `non_functional_requirements`
8. `architecture_integration_data`
9. `security_privacy_regulatory`
10. `delivery_constraints_timeline`
11. `stakeholders_governance`
12. `evaluation_factors_and_risks`

### Required rules
- The analysis prompt set must be versioned.
- The extraction output must contain the fixed analysis items in the exact committed order.
- Each analysis item must include `support_level`, `confidence`, `citations`, and `unknowns`.
- Missing evidence must remain explicit unknowns.
- Do not fabricate answers for unanswered analysis items.
- If citations or page references are unavailable, record that explicitly rather than inventing them.
- The model must not emit server-owned wrapper fields such as `case_id`, source file metadata, `generated_at`, or `model`; the backend adds those during persistence.
- If schema validation fails, reject the case profile.

### Raw current PDF evidence contract
The live case PDF must also produce persisted raw retrieval chunks.

Required chunk fields:
- `case_id`
- `upload_id`
- `page_number`
- `chunk_index`
- `start_offset`
- `end_offset`
- `chunking_version`
- `chunk_hash`
- `content`
- `embedding_model`
- `embedding`
- `index_config_hash`

Required rules:
- Chunking must be deterministic and versioned.
- If chunk contextualization is enabled, the contextualized text becomes the stored indexed chunk content and must be versioned explicitly.
- Raw current PDF retrieval must use persisted chunk records, not ad hoc retrieval-time embedding of full pages.
- Chunk hash, offsets, upload identity, and case identity must prove provenance.
- Indexed artifacts must prove which index-time pipeline config produced them.
- Missing or inconsistent chunk metadata must fail loudly.

## 3. Retrieval contract
Retrieval for a live question should combine:
- structured row-intent features,
- current case-profile signals,
- raw current PDF evidence when relevant,
- structured metadata filters,
- optional keyword or exact-match retrieval.

### Minimum retrieval inputs
For a new RfX question, the system should retrieve against:
- `question_xlsx`
- `context_xlsx`
- `question_xlsx + context_xlsx`
- revision intent when the user is changing content
- relevant `case_profile.analysis_items`
- relevant raw current `pdf_chunks`
- current case signature text / embedding
- metadata such as language, industry, geography, and product fit where available

### Required rules
- Retrieval must be tenant-scoped.
- Retrieval must never cross active case boundaries unless the cross-case source is explicitly historical exemplar data allowed for that tenant.
- Historical exemplars must be labeled as historical.
- Product truth retrieval must be distinguishable from historical-answer retrieval.
- Historical exemplar ranking must use both row similarity (`Context + Question`) and historical client-context similarity (live case signature vs historical case signature).
- Historical `Answer` text may be passed into generation after selection, but it must not be the primary historical recall field.
- Same-source leakage such as matching source row IDs, workbook identities, or paired client-package provenance must be excluded explicitly.
- Style-only revisions must reuse the latest retrieval snapshot unless new facts are introduced; content-changing revisions must refresh retrieval.
- Retrieval snapshots must persist transparent component scores, feature matches, and provenance for every selected evidence item.
- Candidate-generation stages must be explicit and persisted when the system broadens beyond the primary keyword/vector prefilter path.
- Retrieval sufficiency must be explicit and machine-readable (`sufficient`, `weak`, `degraded`, or `insufficient`).
- Retrieval runs must persist resolved pipeline config provenance including config hash and index-config hash.

## 4. Generation contract
Generation is staged.

Answer planning must receive:
- current row context,
- current row question,
- user request / revision,
- normalized selected evidence spanning:
  - current case facts,
  - raw current PDF evidence,
  - product truth,
  - historical exemplars.

Answer rendering must receive:
- the validated `AnswerPlan`,
- the current user request / revision.

For interactive `style_only` revisions, answer rendering may also receive prior thread history from the same row/thread as editorial context only.
That prior thread history must not be treated as evidence.
Content-changing replans, first drafts, and bulk-fill row attempts should render without prior thread history.

The prompting architecture must make authority order explicit:
1. current case facts,
2. product truth,
3. historical exemplars.

Unknowns must remain unknowns. The model must not silently fill gaps.
Historical exemplars are for structure, phrasing, and precedent style; they are not canonical product truth.
If retrieval is weak, degraded, or insufficient, that status must remain explicit in the linked retrieval metadata and review/debug surfaces.
Generation provenance should also remain recoverable through the linked retrieval run, model invocations, and answer version metadata.

## 5. Export contract
When filling a new XLSX questionnaire:
- each answer version must map back to one exact workbook row,
- row IDs must remain stable across revisions,
- exported answers must preserve evidence/provenance linkage.

### Bulk-fill and review contract
Bulk-fill must:
- belong to exactly one case and one questionnaire,
- persist explicit job-level and row-level statuses,
- generate normal `answer_versions` through the grounded row-drafting path,
- preserve retrieval snapshots and evidence links,
- preserve thread/message/retrieval lineage for failed attempts even when no `answer_version` is produced,
- create a fresh row attempt/thread when the workflow is intentionally retrying with a new draft attempt,
- leave generated rows in an explicit review state rather than auto-approving them.

Durable execution rules:
- jobs must be durably queued before execution,
- claim/runner identity must be persisted,
- claim freshness / stale detection must be explicit,
- duplicate active execution must be prevented or surfaced explicitly,
- job and row lifecycle history must be queryable from persisted state, not reconstructed from logs.
- bulk-fill requests must pin the resolved pipeline config they were created under.
- rows with an existing approved answer must not be overwritten by bulk-fill
- rows approved after queueing but before execution must be skipped at execution time and retain their approved answer pointer

Approval must:
- point to one explicit answer version for one explicit row,
- fail loudly if that answer version belongs to another row/case,
- only `answer_versions` are reviewable/exportable artifacts; a thread, message history, or retrieval snapshot alone is not reviewable,
- remain distinguishable from “latest draft”.

### Export modes
Exports must run in one explicit mode:
- `approved_only`
- `latest_available`

Required rules:
- `approved_only` must export the explicit approved answer when one exists and otherwise write a deterministic status placeholder into the answer cell.
- `latest_available` may include unapproved drafts for exportable rows, but that fact must be persisted explicitly in export metadata.
- `latest_available` must export deterministic status placeholders for non-exportable rows such as `rejected`, `failed`, `not_started`, and `skipped`, and for `running`/`needs_review` rows that still have no generated answer.
- Each export must generate aligned XLSX and CSV artifacts from the same resolved row-selection set.
- The primary UI download should be a ZIP bundle containing those XLSX and CSV artifacts.
- Worker/orchestration history may appear in export metadata only as additive explicit metadata; it must not weaken export determinism.

## 6. Failure policy
Fail with actionable diagnostics when:
- workbook columns do not match the contract,
- row IDs cannot be derived,
- provenance metadata is missing,
- case-profile schema validation fails,
- a retrieval query would cross tenant boundaries,
- product truth and historical exemplars are mixed without labeling,
- export cannot prove row mapping,
- a bulk-fill request/job cannot prove its case/questionnaire scope,
- a job claim is duplicated, stale, or orphaned without explicit operator-visible state,
- approval points to an answer version from another row/case,
- `approved_only` export encounters an invalid approved-answer reference or a row marked approved without a valid approved answer.
