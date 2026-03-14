import {
  type CSSProperties,
  FormEvent,
  KeyboardEvent as ReactKeyboardEvent,
  PointerEvent as ReactPointerEvent,
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import {
  approveRow,
  browseDevTable,
  cancelBulkFill,
  createCase,
  downloadUpload,
  draftAnswer,
  exportCase,
  getCaseDetail,
  getSessionContext,
  getThread,
  listAnswerVersions,
  listCases,
  listDevTables,
  rejectRow,
  resumeBulkFill,
  requestBulkFill,
  retryFailedBulkFill,
} from "./lib/api";
import type {
  AnswerVersion,
  CaseDetail,
  CaseSummary,
  ChatMessage,
  DevTableRowsResponse,
  DevTableSummary,
  DraftResponse,
  Evidence,
  QuestionnaireRow,
  RetrievalSummary,
  SessionContext,
  ThreadDetail,
} from "./types";
import ThemeToggle from "./ThemeToggle";
import { useThemePreference } from "./theme";
import "./styles.css";

type CreateCaseState = {
  name: string;
  clientName: string;
  pdf: File | null;
  questionnaire: File | null;
};

type ViewMode = "workspace" | "data-browser";
type RowFilter =
  | "all"
  | "not_started"
  | "running"
  | "needs_review"
  | "approved"
  | "rejected"
  | "failed"
  | "skipped";
type RowVisualState = "neutral" | "approved" | "approved-stale" | "failed";
type ChatMessageVisualState = "neutral" | "approved" | "after-approved";
type StatusChipTone = "neutral" | "accent" | "success" | "warning" | "danger";
type EvidenceAuthorityKey =
  | "current_case_facts"
  | "current_case_pdf"
  | "product_truth"
  | "historical_exemplar";

type EvidenceAuthorityMeta = {
  key: EvidenceAuthorityKey;
  label: string;
  description: string;
  order: number;
};

type EvidenceAuthorityGroup = {
  meta: EvidenceAuthorityMeta;
  items: Evidence[];
};

type EvidenceDetail = {
  label: string;
  value: string;
};

type HistoricalEvidenceSection = {
  label: string;
  value: string;
};

const emptyCaseState: CreateCaseState = {
  name: "",
  clientName: "",
  pdf: null,
  questionnaire: null,
};

const EVIDENCE_AUTHORITY_META: Record<
  EvidenceAuthorityKey,
  EvidenceAuthorityMeta
> = {
  historical_exemplar: {
    key: "historical_exemplar",
    label: "Historical examples",
    description:
      "Approved past answers that help reviewers compare structure, phrasing, and delivery approach.",
    order: 0,
  },
  current_case_facts: {
    key: "current_case_facts",
    label: "Current case facts",
    description:
      "Structured facts extracted from the active case profile. Use these as the primary grounding source for this case.",
    order: 1,
  },
  product_truth: {
    key: "product_truth",
    label: "Product truth",
    description:
      "Approved vendor facts for product, integration, deployment, and security claims.",
    order: 2,
  },
  current_case_pdf: {
    key: "current_case_pdf",
    label: "Source document excerpts",
    description:
      "Direct excerpts from the current client documents. Use these when the structured profile is not sufficient by itself.",
    order: 3,
  },
};

const DESKTOP_LAYOUT_BREAKPOINT = 1100;
const EVIDENCE_PANEL_DEFAULT_WIDTH = 360;
const EVIDENCE_PANEL_MIN_WIDTH = 320;
const EVIDENCE_PANEL_MAX_WIDTH = 520;
const EVIDENCE_PANEL_RESIZE_STEP = 24;

function renderDevValue(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "object") {
    return JSON.stringify(value, null, 2);
  }
  return String(value);
}

function pluralize(count: number, singular: string, plural: string): string {
  return `${count} ${count === 1 ? singular : plural}`;
}

function humanizeStatus(
  value: string | null | undefined,
  fallback = "none",
): string {
  if (!value) {
    return fallback;
  }
  return value.replaceAll("_", " ");
}

function clampNumber(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function toSentenceCase(value: string): string {
  const normalized = value.trim();
  if (!normalized) {
    return "";
  }
  return `${normalized.charAt(0).toUpperCase()}${normalized.slice(1)}`;
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function summarizeText(
  value: string | null | undefined,
  maxLength = 180,
): string {
  const normalized = value?.trim() ?? "";
  if (!normalized) {
    return "No context available.";
  }
  if (normalized.length <= maxLength) {
    return normalized;
  }
  return `${normalized.slice(0, maxLength - 1).trimEnd()}…`;
}

function statusToneForReviewStatus(
  status: string | null | undefined,
): StatusChipTone {
  switch (status) {
    case "approved":
      return "success";
    case "needs_review":
    case "running":
      return "warning";
    case "failed":
    case "rejected":
      return "danger";
    case "not_started":
    case "skipped":
      return "neutral";
    default:
      return "accent";
  }
}

function reviewStatusLabel(
  status: string | null | undefined,
  options: {
    hasApprovedAnswer: boolean;
  },
): string {
  if (status === "rejected") {
    return options.hasApprovedAnswer ? "row rejected" : "rejected";
  }
  return humanizeStatus(status, "not set");
}

function evidencePanelMaxWidthForWorkspace(
  workspaceWidth: number | null,
): number {
  if (workspaceWidth === null) {
    return EVIDENCE_PANEL_MAX_WIDTH;
  }
  return clampNumber(
    Math.floor(workspaceWidth * 0.42),
    EVIDENCE_PANEL_MIN_WIDTH,
    EVIDENCE_PANEL_MAX_WIDTH,
  );
}

function statusToneForAttemptState(
  state: QuestionnaireRow["latest_attempt_state"] | undefined,
): StatusChipTone {
  switch (state) {
    case "answer_available":
      return "success";
    case "pending_no_answer":
      return "warning";
    case "failed_no_answer":
      return "danger";
    default:
      return "neutral";
  }
}

function statusToneForBulkFill(
  status: string | null | undefined,
): StatusChipTone {
  switch (status) {
    case "drafted":
    case "completed":
      return "success";
    case "running":
    case "queued":
    case "needs_review":
      return "warning";
    case "failed":
    case "cancelled":
    case "orphaned":
      return "danger";
    default:
      return "neutral";
  }
}

function statusToneForSufficiency(
  sufficiency: RetrievalSummary["sufficiency"] | "not_run" | undefined,
): StatusChipTone {
  switch (sufficiency) {
    case "sufficient":
      return "success";
    case "weak":
    case "degraded":
      return "warning";
    case "insufficient":
      return "danger";
    default:
      return "neutral";
  }
}

function evidenceAuthorityForItem(item: Evidence): EvidenceAuthorityMeta {
  switch (item.source_kind) {
    case "case_profile_item":
      return EVIDENCE_AUTHORITY_META.current_case_facts;
    case "pdf_chunk":
    case "pdf_page":
      return EVIDENCE_AUTHORITY_META.current_case_pdf;
    case "product_truth_chunk":
      return EVIDENCE_AUTHORITY_META.product_truth;
    case "historical_qa_row":
      return EVIDENCE_AUTHORITY_META.historical_exemplar;
  }
}

function recommendedNextStep(
  row: QuestionnaireRow | null,
  thread: ThreadDetail | null,
  hasGeneratedAnswer: boolean,
  hasApprovedAnswer: boolean,
): string {
  if (!row) {
    return "Select a row from the operational queue to inspect its drafting lineage.";
  }
  if (thread?.thread_state === "failed_no_answer") {
    return "Inspect the failed attempt, then retry generation or fall back to an earlier answer version.";
  }
  if (!hasGeneratedAnswer) {
    return "Generate the first grounded answer for this row.";
  }
  if (!hasApprovedAnswer) {
    return "Review the latest draft, then approve it or reject it for follow-up.";
  }
  if (row.review_status === "approved") {
    return "The approved answer is canonical for export until you explicitly move the approval pointer.";
  }
  return "Decide whether the selected version should replace the current approved pointer.";
}

function rowQueueAriaLabel(row: QuestionnaireRow): string {
  const suffix = row.last_bulk_fill_status
    ? `${row.last_bulk_fill_status} · attempt ${row.last_bulk_fill_attempt_number}`
    : "no bulk fill";
  return `Row ${row.source_row_number} ${row.question} ${row.review_status} ${suffix}`;
}

function rejectActionLabel(options: {
  selectedVersionId: string | null;
  approvedVersionId: string | null | undefined;
  hasApprovedAnswer: boolean;
}): string {
  if (!options.selectedVersionId) {
    return "Mark row rejected";
  }
  if (
    options.approvedVersionId &&
    options.selectedVersionId === options.approvedVersionId
  ) {
    return "Reject row and clear approved answer";
  }
  if (options.hasApprovedAnswer) {
    return "Reject row, keep approved answer";
  }
  return "Mark row rejected";
}

function rejectActionHint(options: {
  selectedVersionId: string | null;
  approvedVersionId: string | null | undefined;
  hasApprovedAnswer: boolean;
}): string {
  if (!options.selectedVersionId) {
    return "Rejection is a row-level review state.";
  }
  if (
    options.approvedVersionId &&
    options.selectedVersionId === options.approvedVersionId
  ) {
    return "This will mark the row rejected and remove the approved pointer.";
  }
  if (options.hasApprovedAnswer) {
    return "This marks the row rejected but keeps the approved answer pointer on the other version.";
  }
  return "This marks the row rejected. It does not delete answer history.";
}

function formatMetadataValue(value: unknown): string {
  if (Array.isArray(value)) {
    return value.map((item) => formatMetadataValue(item)).join(", ");
  }
  if (typeof value === "object" && value !== null) {
    return JSON.stringify(value);
  }
  return String(value);
}

function formatEvidenceTitle(title: string): string {
  const normalized = title.trim().replaceAll("_", " ");
  return normalized ? toSentenceCase(normalized) : "Untitled evidence";
}

function humanizeIdentifier(value: string): string {
  const normalized = value.trim().replace(/[_-]+/g, " ");
  if (!normalized) {
    return "";
  }
  return normalized
    .split(/\s+/)
    .map((part) =>
      part ? `${part.charAt(0).toUpperCase()}${part.slice(1)}` : part,
    )
    .join(" ");
}

function evidenceTraceLabel(item: Evidence): string {
  const provenance = isPlainRecord(item.metadata.provenance)
    ? item.metadata.provenance
    : null;
  switch (item.source_kind) {
    case "case_profile_item":
      return "Structured case fact";
    case "pdf_chunk":
      return "Source document excerpt";
    case "pdf_page":
      return "Source document excerpt";
    case "product_truth_chunk":
      return "Product truth excerpt";
    case "historical_qa_row":
      if (provenance && typeof provenance.source_row_number === "number") {
        return `Historical example · row ${provenance.source_row_number}`;
      }
      return "Historical example";
  }
}

function evidenceDisplayTitle(item: Evidence): string {
  const provenance = isPlainRecord(item.metadata.provenance)
    ? item.metadata.provenance
    : null;

  if (item.source_kind === "historical_qa_row" && provenance) {
    const clientLabel =
      typeof provenance.client_slug === "string"
        ? humanizeIdentifier(provenance.client_slug)
        : "";
    const rowNumber =
      typeof provenance.source_row_number === "number"
        ? provenance.source_row_number
        : null;
    if (clientLabel && rowNumber !== null) {
      return `${clientLabel} · Row ${rowNumber}`;
    }
    if (clientLabel) {
      return clientLabel;
    }
  }

  if (
    (item.source_kind === "pdf_chunk" || item.source_kind === "pdf_page") &&
    provenance &&
    typeof provenance.page_number === "number"
  ) {
    const chunkIndex =
      typeof provenance.chunk_index === "number"
        ? provenance.chunk_index
        : null;
    return chunkIndex !== null
      ? `Page ${provenance.page_number}, chunk ${chunkIndex}`
      : `Page ${provenance.page_number}`;
  }

  return formatEvidenceTitle(item.source_title);
}

function formatEvidenceExcerpt(item: Evidence): string {
  if (item.source_kind !== "historical_qa_row") {
    return item.excerpt;
  }
  return item.excerpt
    .replace(/\n(?=Historical question:)/g, "\n\n")
    .replace(/\n(?=Historical answer exemplar:)/g, "\n\n");
}

function parseHistoricalEvidenceExcerpt(
  excerpt: string,
): HistoricalEvidenceSection[] | null {
  const match = excerpt.match(
    /^Historical client context:\s*([\s\S]*?)\n+Historical question:\s*([\s\S]*?)\n+Historical answer exemplar:\s*([\s\S]*)$/,
  );
  if (!match) {
    return null;
  }
  const [, context, question, answer] = match;
  return [
    {
      label: "Historical client context",
      value: context.trim(),
    },
    {
      label: "Historical question",
      value: question.trim(),
    },
    {
      label: "Historical answer exemplar",
      value: answer.trim(),
    },
  ];
}

function evidenceDetails(item: Evidence): EvidenceDetail[] {
  const details: EvidenceDetail[] = [];
  const metadata = item.metadata;
  const provenance = isPlainRecord(metadata.provenance)
    ? metadata.provenance
    : null;

  if (item.source_kind === "case_profile_item") {
    if (typeof metadata.confidence === "string") {
      details.push({
        label: "Confidence",
        value: toSentenceCase(humanizeStatus(metadata.confidence)),
      });
    }
    if (Array.isArray(metadata.citations) && metadata.citations.length) {
      details.push({
        label: "Citations",
        value: metadata.citations.map((citation) => String(citation)).join(", "),
      });
    }
  }

  if (item.source_kind === "pdf_chunk" || item.source_kind === "pdf_page") {
    const pageNumber =
      provenance && typeof provenance.page_number === "number"
        ? provenance.page_number
        : null;
    const chunkIndex =
      provenance && typeof provenance.chunk_index === "number"
        ? provenance.chunk_index
        : null;
    if (pageNumber !== null) {
      details.push({
        label: "Location",
        value:
          chunkIndex !== null
            ? `Page ${pageNumber}, chunk ${chunkIndex}`
            : `Page ${pageNumber}`,
      });
    }
    if (provenance && typeof provenance.chunking_version === "string") {
      details.push({
        label: "Chunking",
        value: provenance.chunking_version,
      });
    }
  }

  if (item.source_kind === "product_truth_chunk") {
    if (typeof metadata.language === "string") {
      details.push({
        label: "Language",
        value: String(metadata.language).toUpperCase(),
      });
    }
    if (provenance && typeof provenance.source_section === "string") {
      details.push({
        label: "Section",
        value: toSentenceCase(
          provenance.source_section.replaceAll("_", " "),
        ),
      });
    }
    if (provenance && typeof provenance.version === "string") {
      details.push({
        label: "Version",
        value: provenance.version,
      });
    }
  }

  if (item.source_kind === "historical_qa_row") {
    if (provenance && typeof provenance.workbook_file_name === "string") {
      details.push({
        label: "Workbook",
        value: provenance.workbook_file_name,
      });
    }
    if (provenance && typeof provenance.client_name === "string") {
      details.push({
        label: "Client",
        value: provenance.client_name,
      });
    }
  }

  if (details.length) {
    return details;
  }

  return Object.entries(metadata)
    .filter(
      ([key, value]) =>
        !["component_scores", "matched_features", "provenance"].includes(key) &&
        value !== null &&
        value !== undefined &&
        value !== "",
    )
    .slice(0, 2)
    .map(([key, value]) => ({
      label: toSentenceCase(key.replaceAll("_", " ")),
      value: formatMetadataValue(value),
    }));
}

function generationPathLabel(path: string | null | undefined): string {
  switch (path) {
    case "render_only_reuse_plan":
      return "wording revision";
    case "two_stage_plan_render":
      return "grounded draft";
    default:
      return humanizeStatus(path, "draft");
  }
}

function retrievalActionLabel(action: string | null | undefined): string {
  switch (action) {
    case "reuse_retrieval":
    case "reuse_previous_snapshot":
      return "evidence reused";
    case "refresh_retrieval":
      return "evidence refreshed";
    default:
      return humanizeStatus(action, "retrieval");
  }
}

function revisionModeLabel(mode: string | null | undefined): string {
  switch (mode) {
    case "style_only":
      return "wording-only revision";
    case "content_change":
      return "content refresh";
    case "initial_draft":
      return "first draft";
    default:
      return humanizeStatus(mode, "drafting mode");
  }
}

function queueSummaryTooltip(
  kind: "approved" | "needs_review" | "failed" | "not_started",
  count: number,
): string {
  if (count === 0) {
    switch (kind) {
      case "approved":
        return "No rows currently have an approved answer selected for sign-off and approved-only export.";
      case "needs_review":
        return "No rows are currently waiting for reviewer approval or rejection.";
      case "failed":
        return "No rows are currently sitting in a failed state.";
      case "not_started":
        return "Every row in scope has already had some drafting activity.";
    }
  }
  const countLabel = pluralize(count, "row", "rows");
  const verb = count === 1 ? "has" : "have";
  switch (kind) {
    case "approved":
      return `${countLabel} currently ${verb} an approved answer selected for sign-off and approved-only export.`;
    case "needs_review":
      return `${countLabel} ${verb} draft content ready, but a reviewer still needs to approve or reject ${count === 1 ? "it" : "them"}.`;
    case "failed":
      return `${countLabel} ${count === 1 ? "ended" : "ended"} without a usable draft and ${count === 1 ? "needs" : "need"} retry or manual follow-up.`;
    case "not_started":
      return `${countLabel} ${count === 1 ? "has" : "have"} not been drafted yet in this case.`;
  }
}

function reviewStatusTooltip(
  status: string | null | undefined,
  options: {
    hasApprovedAnswer: boolean;
  },
): string {
  switch (status) {
    case "approved":
      return "This row has an approved answer selected for sign-off and export.";
    case "needs_review":
      return "This row has a draft answer available, but it still needs a reviewer decision.";
    case "running":
      return "This row is currently being drafted or retried.";
    case "failed":
      return "The latest drafting attempt for this row did not produce a usable answer.";
    case "rejected":
      return options.hasApprovedAnswer
        ? "This row is marked rejected for follow-up, but an older approved answer still exists."
        : "This row was reviewed and rejected, so it is not approved for export.";
    case "not_started":
      return "No drafting attempt has been started for this row yet.";
    case "skipped":
      return "This row was intentionally skipped in the current workflow.";
    default:
      return "This chip shows the current review state of the row.";
  }
}

function attemptStateTooltip(
  state: QuestionnaireRow["latest_attempt_state"] | undefined,
): string {
  switch (state) {
    case "answer_available":
      return "The latest drafting attempt produced a draft answer that you can review.";
    case "pending_no_answer":
      return "A drafting attempt exists for this row, but it has not produced a usable answer yet.";
    case "failed_no_answer":
      return "The latest drafting attempt ended without a usable answer.";
    default:
      return "No drafting attempt has been recorded for this row yet.";
  }
}

function retrievalSufficiencyTooltip(
  sufficiency: RetrievalSummary["sufficiency"] | "not_run" | undefined,
): string {
  switch (sufficiency) {
    case "sufficient":
      return "The evidence set looks strong enough to support a grounded draft.";
    case "weak":
      return "The system found some useful evidence, but reviewers should check the answer closely.";
    case "degraded":
      return "The evidence set is thinner or less direct than normal, so confidence is lower.";
    case "insufficient":
      return "The system could not find enough reliable evidence to support a strong draft.";
    default:
      return "Evidence has not been collected for this row yet.";
  }
}

function bulkFillStatusTooltip(status: string | null | undefined): string {
  switch (status) {
    case "queued":
      return "This bulk-fill request is waiting to start.";
    case "running":
      return "The system is currently drafting rows for this case in the background.";
    case "drafted":
      return "The run created draft answers. They still need human review and approval before export.";
    case "needs_review":
      return "Bulk fill produced draft answers that now need reviewer sign-off.";
    case "completed":
      return "The latest bulk-fill run finished its drafting work.";
    case "completed_with_failures":
      return "The run finished, but some rows still failed and need follow-up.";
    case "failed":
      return "The bulk-fill run stopped before it could finish the case. Review the failed rows, then retry or resume as needed.";
    case "cancel_requested":
      return "Stop has been requested. The run may still finish the row it is working on.";
    case "cancelled":
      return "The bulk-fill run was stopped before completion.";
    case "orphaned":
      return "The run lost its worker before finishing and needs follow-up.";
    default:
      return "This chip shows the latest bulk-fill state for the case or row.";
  }
}

function exportApprovedTooltip(): string {
  return "Download the questionnaire using only rows whose approved answer has been explicitly signed off. Unapproved drafts are ignored.";
}

function exportLatestTooltip(): string {
  return "Download the questionnaire using the newest available answer on each row, even if some rows are still unapproved.";
}

function launchBulkFillTooltip(): string {
  return "Start a case-wide drafting run that creates row drafts from the current evidence. Review and approval still stay with a human reviewer.";
}

function rowBackgroundTooltip(): string {
  return "This is the source background text attached to the selected questionnaire row. It explains the client situation and scope that should shape the answer.";
}

function bulkFillExecutionModeTooltip(mode: string | null | undefined): string {
  if (!mode) {
    return "No processing mode has been recorded for this bulk-fill run yet.";
  }
  return `This says how the bulk-fill run was started and processed. It affects job handling, not answer wording. Current mode: ${humanizeStatus(mode)}.`;
}

function bulkFillRunnerTooltip(runnerId: string | null | undefined): string {
  if (!runnerId) {
    return "No worker has claimed this bulk-fill request yet.";
  }
  return "This is the internal worker label for the process currently handling or last handling the bulk-fill request.";
}

function profileMissingTooltip(): string {
  return "The structured case profile has not been created yet, so the main case-facts layer is still missing.";
}

function approvalStateTooltip(options: {
  hasApprovedAnswer: boolean;
  selectedAnswerIsApproved: boolean;
}): string {
  if (!options.hasApprovedAnswer) {
    return "This row does not have an approved answer yet.";
  }
  if (options.selectedAnswerIsApproved) {
    return "The version on screen is already the approved answer for this row.";
  }
  return "A different version is currently approved. The row keeps that approved answer until you move the approval pointer.";
}

function rowBulkFillAttemptTooltip(
  status: string | null | undefined,
  attemptNumber: number | null | undefined,
): string {
  if (!status) {
    return "Bulk fill has not touched this row yet.";
  }
  const attemptLabel =
    attemptNumber !== null && attemptNumber !== undefined
      ? ` on attempt ${attemptNumber}`
      : "";
  return `Bulk fill last touched this row${attemptLabel} and left it in ${humanizeStatus(status)} state.`;
}

function generationPathTooltip(path: string | null | undefined): string {
  switch (path) {
    case "render_only_reuse_plan":
      return "This version is a wording revision of an existing answer.";
    case "two_stage_plan_render":
      return "This version was produced as a full grounded draft from the current evidence set.";
    default:
      return "This shows what kind of drafting path created this answer version.";
  }
}

function retrievalActionTooltip(action: string | null | undefined): string {
  switch (action) {
    case "reuse_retrieval":
    case "reuse_previous_snapshot":
      return "The system reused the earlier evidence set because the request was treated as a wording change.";
    case "refresh_retrieval":
      return "The system collected a fresh evidence set because the content needed to be drafted or materially changed.";
    default:
      return "This says whether the evidence set was reused or freshly collected for the current draft.";
  }
}

function revisionModeTooltip(mode: string | null | undefined): string {
  switch (mode) {
    case "style_only":
      return "This draft keeps the same substance and focuses on wording changes.";
    case "content_change":
      return "This draft was treated as a content change, so evidence may have been refreshed.";
    case "initial_draft":
      return "This is the first drafting pass for the selected row.";
    default:
      return "This says what kind of drafting action produced the current answer.";
  }
}

function broadenedTooltip(): string {
  return "The search had to cast a wider net than normal to find enough usable evidence.";
}

function degradedTooltip(): string {
  return "The evidence set was built under weaker conditions than normal, so reviewers should check it more carefully.";
}

function answerVersionTooltip(versionNumber: number): string {
  return `This message is linked to answer version ${versionNumber} in the row's revision history.`;
}

function approvedPointerTooltip(): string {
  return "This version is the one currently selected as the approved answer for sign-off and export.";
}

function canonicalTooltip(): string {
  return "This approved answer is the version the system will use for approved-only export until a reviewer changes it.";
}

function relevanceTooltip(score: number): string {
  return `This score shows how directly the evidence matched the selected row. Higher values mean the system considered it more useful for this draft. Current score: ${score.toFixed(3)}.`;
}

function timelineEntryTitle(
  item: ChatMessage,
  assistantIndex: number,
  userIndex: number,
): string {
  if (item.role === "user") {
    return userIndex === 0 ? "Draft request" : "Revision request";
  }
  if (item.role === "assistant") {
    return assistantIndex === 0 ? "Generated draft" : "Revised draft";
  }
  return humanizeStatus(item.role, "timeline event");
}

function timelineEntryEyebrow(item: ChatMessage): string {
  if (item.role === "assistant") {
    return "Draft output";
  }
  if (item.role === "user") {
    return "Reviewer instruction";
  }
  return humanizeStatus(item.role, "event");
}

function StatusChip({
  label,
  tone,
  muted = false,
  tooltip,
}: {
  label: string;
  tone: StatusChipTone;
  muted?: boolean;
  tooltip: string;
}) {
  return (
    <span
      className={muted ? "status-chip muted" : "status-chip"}
      data-tone={tone}
      title={tooltip}
      aria-description={tooltip}
    >
      <span className="status-chip-indicator" aria-hidden="true" />
      {label}
    </span>
  );
}

function MetaBadge({ label, tooltip }: { label: string; tooltip: string }) {
  return (
    <span className="meta-badge" title={tooltip} aria-description={tooltip}>
      {label}
    </span>
  );
}

function SidebarEdgeToggleIcon({
  collapsed,
  side = "left",
}: {
  collapsed: boolean;
  side?: "left" | "right";
}) {
  const chevronLeft = "M10 3.5 5.5 8 10 12.5";
  const chevronRight = "M6 3.5 10.5 8 6 12.5";
  return (
    <svg
      aria-hidden="true"
      viewBox="0 0 16 16"
      className="sidebar-edge-toggle-icon"
    >
      <path
        d={
          side === "left"
            ? collapsed
              ? chevronRight
              : chevronLeft
            : collapsed
              ? chevronLeft
              : chevronRight
        }
        fill="none"
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.6"
      />
    </svg>
  );
}

function latestAttemptThreadId(
  detail: CaseDetail,
  row: QuestionnaireRow,
): string | null {
  return (
    row.latest_attempt_thread_id ??
    detail.chats.find((thread) => thread.questionnaire_row_id === row.id)?.id ??
    null
  );
}

function latestAttemptState(
  detail: CaseDetail,
  row: QuestionnaireRow,
): QuestionnaireRow["latest_attempt_state"] {
  if (row.latest_attempt_state) {
    return row.latest_attempt_state;
  }
  return latestAttemptThreadId(detail, row)
    ? row.current_answer.trim().length > 0
      ? "answer_available"
      : "pending_no_answer"
    : "none";
}

function threadDetailFromDraftResponse(result: DraftResponse): ThreadDetail {
  return {
    thread: result.thread,
    thread_state: "answer_available",
    messages: result.messages,
    answer_version: result.answer_version,
    retrieval: result.retrieval,
    evidence: result.evidence,
    failure_detail: null,
  };
}

function threadLabelForAnswerVersion(
  version: AnswerVersion,
  fallbackThreadId: string | null,
): string | undefined {
  return version.chat_thread_id ?? fallbackThreadId ?? undefined;
}

function attemptStateLabel(
  state: QuestionnaireRow["latest_attempt_state"] | undefined,
): string {
  return (state ?? "none").replaceAll("_", " ");
}

function rowVisualState(row: QuestionnaireRow): RowVisualState {
  const latestAttemptState = row.latest_attempt_state ?? "none";
  if (row.approved_answer_version_id) {
    return row.review_status === "approved" &&
      latestAttemptState !== "pending_no_answer" &&
      latestAttemptState !== "failed_no_answer"
      ? "approved"
      : "approved-stale";
  }
  if (
    row.review_status === "failed" ||
    latestAttemptState === "failed_no_answer"
  ) {
    return "failed";
  }
  return "neutral";
}

function App() {
  const devPanelsEnabled = import.meta.env.VITE_ENABLE_DEV_PANELS === "true";
  const { preference, setPreference } = useThemePreference();
  const [session, setSession] = useState<SessionContext | null>(null);
  const [cases, setCases] = useState<CaseSummary[]>([]);
  const [selectedCase, setSelectedCase] = useState<CaseDetail | null>(null);
  const [selectedRow, setSelectedRow] = useState<QuestionnaireRow | null>(null);
  const [threadState, setThreadState] = useState<ThreadDetail | null>(null);
  const [answerVersions, setAnswerVersions] = useState<AnswerVersion[]>([]);
  const [selectedAnswerVersionId, setSelectedAnswerVersionId] = useState<
    string | null
  >(null);
  const [message, setMessage] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [status, setStatus] = useState<string>("Loading session...");
  const [formState, setFormState] = useState<CreateCaseState>(emptyCaseState);
  const [isDrafting, setIsDrafting] = useState(false);
  const [viewMode, setViewMode] = useState<ViewMode>("workspace");
  const [devTables, setDevTables] = useState<DevTableSummary[]>([]);
  const [selectedDevTable, setSelectedDevTable] = useState<string | null>(null);
  const [devTableRows, setDevTableRows] = useState<DevTableRowsResponse | null>(
    null,
  );
  const [isDevLoading, setIsDevLoading] = useState(false);
  const [useSelectedCaseFilter, setUseSelectedCaseFilter] = useState(true);
  const [rowFilter, setRowFilter] = useState<RowFilter>("all");
  const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(false);
  const [isEvidenceCollapsed, setIsEvidenceCollapsed] = useState(false);
  const [isCaseFormVisible, setIsCaseFormVisible] = useState(false);
  const [isCaseProfileExpanded, setIsCaseProfileExpanded] = useState(false);
  const [isContextExpanded, setIsContextExpanded] = useState(false);
  const [reviewViewMode, setReviewViewMode] = useState<
    "draft" | "compare" | "history"
  >("draft");
  const [draftActionMode, setDraftActionMode] = useState<
    "revise" | "regenerate" | null
  >(null);
  const [expandedEvidenceIds, setExpandedEvidenceIds] = useState<string[]>([]);
  const [evidencePanelWidth, setEvidencePanelWidth] = useState(
    EVIDENCE_PANEL_DEFAULT_WIDTH,
  );
  const [workspaceWidth, setWorkspaceWidth] = useState<number | null>(null);
  const [chatViewportHeight, setChatViewportHeight] = useState<number | null>(
    null,
  );
  const [isEvidenceResizing, setIsEvidenceResizing] = useState(false);
  const activeViewMode: ViewMode = devPanelsEnabled ? viewMode : "workspace";
  const workspaceLoadIdRef = useRef(0);
  const workspaceRef = useRef<HTMLElement | null>(null);
  const answerPanelRef = useRef<HTMLElement | null>(null);
  const chatLogRef = useRef<HTMLDivElement | null>(null);
  const composerRef = useRef<HTMLDivElement | null>(null);
  const evidenceResizeStateRef = useRef<{
    startX: number;
    startWidth: number;
  } | null>(null);
  const evidenceResizeCleanupRef = useRef<(() => void) | null>(null);

  const selectedAnswerVersion = useMemo(() => {
    if (!selectedAnswerVersionId) {
      return null;
    }
    return (
      answerVersions.find(
        (version) => version.id === selectedAnswerVersionId,
      ) ?? null
    );
  }, [answerVersions, selectedAnswerVersionId]);

  const inspectedAnswerVersion = useMemo(() => {
    return selectedAnswerVersion ?? threadState?.answer_version ?? null;
  }, [selectedAnswerVersion, threadState]);

  const answerVersionsById = useMemo(
    () => new Map(answerVersions.map((version) => [version.id, version])),
    [answerVersions],
  );

  const approvedAnswerVersion = useMemo(() => {
    if (!selectedRow?.approved_answer_version_id) {
      return null;
    }
    return (
      answerVersions.find(
        (version) => version.id === selectedRow.approved_answer_version_id,
      ) ?? null
    );
  }, [answerVersions, selectedRow]);
  const activeAttemptState = selectedAnswerVersion
    ? "answer_available"
    : selectedCase && selectedRow
      ? latestAttemptState(selectedCase, selectedRow)
      : "none";
  const hasGeneratedAnswer = selectedAnswerVersion !== null;
  const latestChatMessageId =
    threadState?.messages[threadState.messages.length - 1]?.id ?? null;
  const isChatFocusMode = isSidebarCollapsed && isEvidenceCollapsed;
  const hasLongContext = (selectedRow?.context.length ?? 0) > 220;
  const approvedMessageIndex = useMemo(() => {
    const approvedAnswerVersionId = selectedRow?.approved_answer_version_id;
    if (!approvedAnswerVersionId) {
      return -1;
    }
    return (
      threadState?.messages.findIndex(
        (message) => message.answer_version_id === approvedAnswerVersionId,
      ) ?? -1
    );
  }, [selectedRow?.approved_answer_version_id, threadState?.messages]);

  const filteredRows =
    selectedCase?.questionnaire_rows.filter((row) =>
      rowFilter === "all" ? true : row.review_status === rowFilter,
    ) ?? [];
  const caseFormVisible = !selectedCase || isCaseFormVisible;
  const latestBulkFill = selectedCase?.latest_bulk_fill ?? null;
  const bulkFillImpact = useMemo(() => {
    if (!selectedCase) {
      return null;
    }
    const rowIdsWithThreads = new Set(
      selectedCase.chats.map((thread) => thread.questionnaire_row_id),
    );
    let approved = 0;
    let overwrite = 0;
    let untouched = 0;
    for (const row of selectedCase.questionnaire_rows) {
      if (row.approved_answer_version_id) {
        approved += 1;
        continue;
      }
      const hasExistingWork =
        row.current_answer.trim().length > 0 ||
        rowIdsWithThreads.has(row.id) ||
        !["not_started", "skipped"].includes(row.review_status);
      if (hasExistingWork) {
        overwrite += 1;
      } else {
        untouched += 1;
      }
    }
    return { approved, overwrite, untouched };
  }, [selectedCase]);
  const queueCounts = useMemo(() => {
    if (!selectedCase) {
      return null;
    }
    return selectedCase.questionnaire_rows.reduce(
      (accumulator, row) => {
        accumulator.total += 1;
        if (row.review_status === "approved") {
          accumulator.approved += 1;
        }
        if (row.review_status === "needs_review") {
          accumulator.needsReview += 1;
        }
        if (row.review_status === "failed") {
          accumulator.failed += 1;
        }
        if (row.review_status === "not_started") {
          accumulator.notStarted += 1;
        }
        if (
          row.latest_attempt_state === "answer_available" ||
          row.current_answer.trim().length > 0
        ) {
          accumulator.answerAvailable += 1;
        }
        return accumulator;
      },
      {
        total: 0,
        approved: 0,
        needsReview: 0,
        failed: 0,
        notStarted: 0,
        answerAvailable: 0,
      },
    );
  }, [selectedCase]);
  const retrievalSufficiency =
    threadState?.retrieval?.sufficiency ?? ("not_run" as const);
  const hasApprovedAnswer =
    !!selectedRow?.approved_answer_version_id ||
    !!selectedRow?.approved_answer_text;
  const nextStepSummary = recommendedNextStep(
    selectedRow,
    threadState,
    hasGeneratedAnswer,
    hasApprovedAnswer,
  );
  const evidenceAuthorityGroups = useMemo(() => {
    const groups = new Map<EvidenceAuthorityKey, EvidenceAuthorityGroup>();
    for (const item of threadState?.evidence ?? []) {
      const meta = evidenceAuthorityForItem(item);
      const currentGroup = groups.get(meta.key) ?? { meta, items: [] };
      currentGroup.items.push(item);
      groups.set(meta.key, currentGroup);
    }
    return [...groups.values()].sort(
      (left, right) => left.meta.order - right.meta.order,
    );
  }, [threadState?.evidence]);
  const selectedAnswerIsApproved =
    !!selectedAnswerVersion &&
    selectedAnswerVersion.id === selectedRow?.approved_answer_version_id;
  const evidencePanelMaxWidth = useMemo(
    () => evidencePanelMaxWidthForWorkspace(workspaceWidth),
    [workspaceWidth],
  );
  const effectiveEvidencePanelWidth = useMemo(
    () =>
      clampNumber(
        evidencePanelWidth,
        EVIDENCE_PANEL_MIN_WIDTH,
        evidencePanelMaxWidth,
      ),
    [evidencePanelMaxWidth, evidencePanelWidth],
  );
  const workspaceStyle = useMemo<CSSProperties | undefined>(
    () =>
      activeViewMode === "workspace"
        ? ({
            "--workspace-right-column": `${effectiveEvidencePanelWidth}px`,
          } as CSSProperties)
        : undefined,
    [activeViewMode, effectiveEvidencePanelWidth],
  );

  const clearRowWorkspace = useCallback((row: QuestionnaireRow | null) => {
    workspaceLoadIdRef.current += 1;
    setSelectedRow(row);
    setThreadState(null);
    setAnswerVersions([]);
    setSelectedAnswerVersionId(null);
    setMessage("");
  }, []);

  const loadRowArtifacts = useCallback(
    async (detail: CaseDetail, row: QuestionnaireRow) => {
      const requestId = workspaceLoadIdRef.current + 1;
      workspaceLoadIdRef.current = requestId;
      setSelectedRow(row);
      setThreadState(null);
      setAnswerVersions([]);
      setSelectedAnswerVersionId(null);
      setMessage("");
      const threadId = latestAttemptThreadId(detail, row);
      const attemptState = latestAttemptState(detail, row);
      const [versionsResult, threadResult] = await Promise.allSettled([
        listAnswerVersions(detail.id, row.id),
        threadId ? getThread(detail.id, threadId) : Promise.resolve(null),
      ]);
      if (requestId !== workspaceLoadIdRef.current) {
        return;
      }
      if (versionsResult.status === "rejected") {
        throw versionsResult.reason;
      }
      const versions = versionsResult.value;
      setAnswerVersions(versions);
      if (threadResult.status === "rejected") {
        setThreadState(null);
        setError((threadResult.reason as Error).message);
      } else {
        setThreadState(threadResult.value);
      }
      setSelectedAnswerVersionId(
        attemptState === "answer_available" ||
          (attemptState === "none" && versions.length > 0)
          ? threadResult.status === "fulfilled"
            ? (threadResult.value?.answer_version?.id ??
              versions[0]?.id ??
              null)
            : (versions[0]?.id ?? null)
          : null,
      );
    },
    [],
  );

  const refreshSelectedCase = useCallback(
    async (caseId: string, preferredRowId?: string | null) => {
      const detail = await getCaseDetail(caseId);
      setSelectedCase(detail);
      const nextRow =
        (preferredRowId
          ? detail.questionnaire_rows.find((row) => row.id === preferredRowId)
          : null) ??
        detail.questionnaire_rows[0] ??
        null;
      if (nextRow) {
        await loadRowArtifacts(detail, nextRow);
      } else {
        clearRowWorkspace(null);
      }
      return detail;
    },
    [clearRowWorkspace, loadRowArtifacts],
  );

  useEffect(() => {
    let active = true;

    async function loadInitial() {
      try {
        setError(null);
        const [sessionContext, caseList] = await Promise.all([
          getSessionContext(),
          listCases(),
        ]);
        if (!active) {
          return;
        }
        setSession(sessionContext);
        setCases(caseList);
        setIsCaseFormVisible(caseList.length === 0);
        setStatus(
          caseList.length
            ? "Select a case to inspect its rows and chats."
            : "Create the first case.",
        );
        if (caseList[0]) {
          await refreshSelectedCase(caseList[0].id);
          if (!active) {
            return;
          }
        }
      } catch (caught) {
        if (!active) {
          return;
        }
        setError((caught as Error).message);
        setStatus("Unable to load session or cases.");
      }
    }

    void loadInitial();

    return () => {
      active = false;
    };
  }, [refreshSelectedCase]);

  useEffect(() => {
    if (activeViewMode !== "data-browser") {
      return;
    }
    let active = true;

    async function loadDevTables() {
      try {
        setError(null);
        const result = await listDevTables();
        if (!active) {
          return;
        }
        setDevTables(result.tables);
        setSelectedDevTable(
          (current) =>
            current ??
            result.tables.find((table) => table.name === "answer_versions")
              ?.name ??
            result.tables[0]?.name ??
            null,
        );
      } catch (caught) {
        if (!active) {
          return;
        }
        setError((caught as Error).message);
      }
    }

    void loadDevTables();
    return () => {
      active = false;
    };
  }, [activeViewMode]);

  useEffect(() => {
    if (activeViewMode !== "data-browser" || !selectedDevTable) {
      return;
    }
    let active = true;
    const currentTableName = selectedDevTable;

    async function loadDevRows() {
      try {
        setError(null);
        setIsDevLoading(true);
        const selectedTableConfig = devTables.find(
          (table) => table.name === currentTableName,
        );
        const caseId =
          useSelectedCaseFilter &&
          selectedCase &&
          selectedTableConfig?.case_filter_supported
            ? selectedCase.id
            : undefined;
        const rows = await browseDevTable(currentTableName, {
          caseId,
          limit: 50,
        });
        if (!active) {
          return;
        }
        setDevTableRows(rows);
      } catch (caught) {
        if (!active) {
          return;
        }
        setError((caught as Error).message);
        setDevTableRows(null);
      } finally {
        if (active) {
          setIsDevLoading(false);
        }
      }
    }

    void loadDevRows();
    return () => {
      active = false;
    };
  }, [
    devTables,
    selectedCase,
    selectedDevTable,
    useSelectedCaseFilter,
    activeViewMode,
  ]);

  useEffect(() => {
    const activeCaseId = selectedCase?.id;
    if (
      activeViewMode !== "workspace" ||
      !activeCaseId ||
      !latestBulkFill ||
      !["queued", "running", "cancel_requested"].includes(latestBulkFill.status)
    ) {
      return;
    }
    const interval = window.setInterval(() => {
      void refreshSelectedCase(activeCaseId, selectedRow?.id ?? null).catch(
        (caught) => setError((caught as Error).message),
      );
    }, 1000);
    return () => window.clearInterval(interval);
  }, [
    selectedCase?.id,
    latestBulkFill,
    selectedRow?.id,
    activeViewMode,
    refreshSelectedCase,
  ]);

  useEffect(() => {
    if (!chatLogRef.current) {
      return;
    }
    chatLogRef.current.scrollTop = chatLogRef.current.scrollHeight;
  }, [selectedRow?.id, latestChatMessageId]);

  useEffect(() => {
    setIsContextExpanded(false);
  }, [selectedRow?.id]);

  useEffect(() => {
    setDraftActionMode(null);
    setExpandedEvidenceIds([]);
    setReviewViewMode("draft");
  }, [selectedRow?.id]);

  useEffect(() => {
    setEvidencePanelWidth((current) =>
      clampNumber(current, EVIDENCE_PANEL_MIN_WIDTH, evidencePanelMaxWidth),
    );
  }, [evidencePanelMaxWidth]);

  const stopEvidenceResize = useCallback(() => {
    evidenceResizeCleanupRef.current?.();
    evidenceResizeCleanupRef.current = null;
    evidenceResizeStateRef.current = null;
    setIsEvidenceResizing(false);
    document.body.style.removeProperty("cursor");
    document.body.style.removeProperty("user-select");
  }, []);

  useEffect(() => {
    return () => {
      stopEvidenceResize();
    };
  }, [stopEvidenceResize]);

  useEffect(() => {
    if (isEvidenceCollapsed) {
      stopEvidenceResize();
    }
  }, [isEvidenceCollapsed, stopEvidenceResize]);

  const handleEvidenceResizeStart = useCallback(
    (event: ReactPointerEvent<HTMLDivElement>) => {
      if (
        event.button !== 0 ||
        isEvidenceCollapsed ||
        window.innerWidth <= DESKTOP_LAYOUT_BREAKPOINT
      ) {
        return;
      }
      const handlePointerMove = (moveEvent: PointerEvent) => {
        const resizeState = evidenceResizeStateRef.current;
        if (!resizeState) {
          return;
        }
        setEvidencePanelWidth(
          clampNumber(
            resizeState.startWidth + resizeState.startX - moveEvent.clientX,
            EVIDENCE_PANEL_MIN_WIDTH,
            evidencePanelMaxWidth,
          ),
        );
      };
      const handlePointerEnd = () => {
        stopEvidenceResize();
      };
      evidenceResizeCleanupRef.current?.();
      evidenceResizeStateRef.current = {
        startX: event.clientX,
        startWidth: effectiveEvidencePanelWidth,
      };
      evidenceResizeCleanupRef.current = () => {
        window.removeEventListener("pointermove", handlePointerMove);
        window.removeEventListener("pointerup", handlePointerEnd);
        window.removeEventListener("pointercancel", handlePointerEnd);
      };
      window.addEventListener("pointermove", handlePointerMove);
      window.addEventListener("pointerup", handlePointerEnd);
      window.addEventListener("pointercancel", handlePointerEnd);
      document.body.style.cursor = "col-resize";
      document.body.style.userSelect = "none";
      setIsEvidenceResizing(true);
      event.preventDefault();
    },
    [
      effectiveEvidencePanelWidth,
      evidencePanelMaxWidth,
      isEvidenceCollapsed,
      stopEvidenceResize,
    ],
  );

  const handleEvidenceResizeKeyDown = useCallback(
    (event: ReactKeyboardEvent<HTMLDivElement>) => {
      if (window.innerWidth <= DESKTOP_LAYOUT_BREAKPOINT) {
        return;
      }
      switch (event.key) {
        case "ArrowLeft":
          setEvidencePanelWidth((current) =>
            clampNumber(
              current + EVIDENCE_PANEL_RESIZE_STEP,
              EVIDENCE_PANEL_MIN_WIDTH,
              evidencePanelMaxWidth,
            ),
          );
          event.preventDefault();
          break;
        case "ArrowRight":
          setEvidencePanelWidth((current) =>
            clampNumber(
              current - EVIDENCE_PANEL_RESIZE_STEP,
              EVIDENCE_PANEL_MIN_WIDTH,
              evidencePanelMaxWidth,
            ),
          );
          event.preventDefault();
          break;
        case "Home":
          setEvidencePanelWidth(EVIDENCE_PANEL_MIN_WIDTH);
          event.preventDefault();
          break;
        case "End":
          setEvidencePanelWidth(evidencePanelMaxWidth);
          event.preventDefault();
          break;
      }
    },
    [evidencePanelMaxWidth],
  );

  useLayoutEffect(() => {
    function updateChatViewportHeight() {
      if (
        activeViewMode !== "workspace" ||
        !answerPanelRef.current ||
        !chatLogRef.current ||
        window.innerWidth <= DESKTOP_LAYOUT_BREAKPOINT
      ) {
        setChatViewportHeight(null);
        return;
      }
      const panelTop = Math.max(
        answerPanelRef.current.getBoundingClientRect().top,
        24,
      );
      const chatTop = panelTop + chatLogRef.current.offsetTop;
      const composerHeight =
        composerRef.current?.getBoundingClientRect().height ?? 0;
      const gapBetweenChatAndComposer = 6;
      const bottomClearance = 0;
      const availableHeight = Math.floor(
        window.innerHeight -
          chatTop -
          gapBetweenChatAndComposer -
          composerHeight -
          bottomClearance,
      );
      setChatViewportHeight(Math.min(780, Math.max(320, availableHeight)));
    }

    const frameId = window.requestAnimationFrame(updateChatViewportHeight);
    window.addEventListener("resize", updateChatViewportHeight);
    return () => {
      window.cancelAnimationFrame(frameId);
      window.removeEventListener("resize", updateChatViewportHeight);
    };
  }, [
    activeViewMode,
    effectiveEvidencePanelWidth,
    hasGeneratedAnswer,
    isContextExpanded,
    isEvidenceCollapsed,
    isSidebarCollapsed,
    latestChatMessageId,
    selectedRow?.id,
  ]);

  useLayoutEffect(() => {
    function updateWorkspaceWidth() {
      if (
        activeViewMode !== "workspace" ||
        !workspaceRef.current ||
        window.innerWidth <= DESKTOP_LAYOUT_BREAKPOINT
      ) {
        setWorkspaceWidth(null);
        return;
      }
      const measuredWidth = Math.floor(
        workspaceRef.current.getBoundingClientRect().width,
      );
      setWorkspaceWidth(measuredWidth > 0 ? measuredWidth : window.innerWidth);
    }

    const frameId = window.requestAnimationFrame(updateWorkspaceWidth);
    window.addEventListener("resize", updateWorkspaceWidth);
    return () => {
      window.cancelAnimationFrame(frameId);
      window.removeEventListener("resize", updateWorkspaceWidth);
    };
  }, [activeViewMode, isEvidenceCollapsed, isSidebarCollapsed]);

  async function selectCase(caseId: string) {
    try {
      setError(null);
      setIsCaseFormVisible(false);
      await refreshSelectedCase(caseId, selectedRow?.id ?? null);
    } catch (caught) {
      setError((caught as Error).message);
    }
  }

  async function selectRow(detail: CaseDetail, row: QuestionnaireRow) {
    setError(null);
    await loadRowArtifacts(detail, row);
  }

  async function inspectAnswerVersion(version: AnswerVersion) {
    if (!selectedCase) {
      return;
    }
    setSelectedAnswerVersionId(version.id);
    const threadId = threadLabelForAnswerVersion(
      version,
      threadState?.thread.id ?? null,
    );
    if (!threadId || threadId === threadState?.thread.id) {
      return;
    }
    const requestId = workspaceLoadIdRef.current + 1;
    workspaceLoadIdRef.current = requestId;
    try {
      setError(null);
      const nextThread = await getThread(selectedCase.id, threadId);
      if (requestId !== workspaceLoadIdRef.current) {
        return;
      }
      setThreadState(nextThread);
    } catch (caught) {
      if (requestId !== workspaceLoadIdRef.current) {
        return;
      }
      setThreadState(null);
      setError((caught as Error).message);
    }
  }

  async function handleCreateCase(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!formState.pdf) {
      setError("A client PDF is required.");
      return;
    }
    try {
      setError(null);
      setStatus("Creating case and extracting case_profile...");
      const created = await createCase({
        name: formState.name,
        clientName: formState.clientName,
        pdf: formState.pdf,
        questionnaire: formState.questionnaire,
      });
      setFormState(emptyCaseState);
      setIsCaseFormVisible(false);
      const refreshedCases = await listCases();
      setCases(refreshedCases);
      setSelectedCase(created);
      setStatus("Case created.");
      if (created.questionnaire_rows[0]) {
        await selectRow(created, created.questionnaire_rows[0]);
      }
    } catch (caught) {
      setError((caught as Error).message);
      setStatus("Case creation failed.");
    }
  }

  async function handleReviseAnswer() {
    if (
      !selectedCase ||
      !selectedRow ||
      !hasGeneratedAnswer ||
      !message.trim() ||
      isDrafting ||
      !inspectedAnswerVersion
    ) {
      return;
    }
    try {
      setError(null);
      setIsDrafting(true);
      setStatus("Waiting for model response...");
      const result = await draftAnswer(
        selectedCase.id,
        selectedRow.id,
        message,
        threadLabelForAnswerVersion(
          inspectedAnswerVersion,
          threadState?.thread.id ?? null,
        ),
        "style_only",
      );
      setThreadState(threadDetailFromDraftResponse(result));
      setSelectedAnswerVersionId(result.answer_version.id);
      setMessage("");
      setDraftActionMode(null);
      setStatus("Answer revised.");
      await refreshSelectedCase(selectedCase.id, selectedRow.id);
    } catch (caught) {
      setError((caught as Error).message);
      setStatus("Revision failed.");
    } finally {
      setIsDrafting(false);
    }
  }

  async function handleExport(mode: "approved_only" | "latest_available") {
    if (!selectedCase) {
      return;
    }
    try {
      setError(null);
      const result = await exportCase(selectedCase.id, mode);
      await downloadUpload(result.zip_download_upload_id);
      const exportDetails =
        result.export_mode === "approved_only"
          ? [
              pluralize(
                result.placeholder_row_count,
                "placeholder row",
                "placeholder rows",
              ),
            ]
          : [
              ...(result.includes_unapproved_drafts ? ["includes drafts"] : []),
              ...(result.placeholder_row_count > 0
                ? [
                    pluralize(
                      result.placeholder_row_count,
                      "placeholder row",
                      "placeholder rows",
                    ),
                  ]
                : []),
            ];
      const exportDetail = exportDetails.length
        ? ` (${exportDetails.join(", ")})`
        : "";
      setStatus(
        `Export downloaded with mode ${result.export_mode}${exportDetail}.`,
      );
    } catch (caught) {
      setError((caught as Error).message);
    }
  }

  async function handleBulkFill() {
    if (!selectedCase) {
      return;
    }
    try {
      setError(null);
      if (bulkFillImpact && bulkFillImpact.overwrite > 0) {
        const confirmed = window.confirm(
          [
            "Bulk-fill will keep approved rows unchanged and replace existing unapproved work with fresh generated drafts.",
            `Keep ${pluralize(bulkFillImpact.approved, "approved row", "approved rows")}.`,
            `Replace ${pluralize(bulkFillImpact.overwrite, "unapproved row with existing work", "unapproved rows with existing work")}.`,
            `Generate ${pluralize(bulkFillImpact.untouched, "untouched row", "untouched rows")}.`,
            "",
            "Continue?",
          ].join("\n"),
        );
        if (!confirmed) {
          setStatus("Bulk-fill launch cancelled.");
          return;
        }
      }
      const result = await requestBulkFill(selectedCase.id);
      setStatus(`Bulk-fill queued with status ${result.request.status}.`);
      const detail = await refreshSelectedCase(
        selectedCase.id,
        selectedRow?.id ?? null,
      );
      if (detail.latest_bulk_fill?.id === result.request.id) {
        setStatus(`Bulk-fill request ${detail.latest_bulk_fill.status}.`);
      }
    } catch (caught) {
      setError((caught as Error).message);
    }
  }

  async function handleRetryFailedBulkFill() {
    if (!selectedCase?.latest_bulk_fill) {
      return;
    }
    try {
      setError(null);
      const result = await retryFailedBulkFill(
        selectedCase.id,
        selectedCase.latest_bulk_fill.id,
      );
      setStatus(`Retry launched with status ${result.request.status}.`);
      await refreshSelectedCase(selectedCase.id, selectedRow?.id ?? null);
    } catch (caught) {
      setError((caught as Error).message);
    }
  }

  async function handleResumeBulkFill() {
    if (!selectedCase?.latest_bulk_fill) {
      return;
    }
    try {
      setError(null);
      const result = await resumeBulkFill(
        selectedCase.id,
        selectedCase.latest_bulk_fill.id,
      );
      setStatus(`Resume launched with status ${result.request.status}.`);
      await refreshSelectedCase(selectedCase.id, selectedRow?.id ?? null);
    } catch (caught) {
      setError((caught as Error).message);
    }
  }

  async function handleCancelBulkFill() {
    if (!selectedCase?.latest_bulk_fill) {
      return;
    }
    try {
      setError(null);
      const result = await cancelBulkFill(
        selectedCase.id,
        selectedCase.latest_bulk_fill.id,
      );
      setStatus(`Bulk-fill updated to ${result.request.status}.`);
      await refreshSelectedCase(selectedCase.id, selectedRow?.id ?? null);
    } catch (caught) {
      setError((caught as Error).message);
    }
  }

  async function handleApproveSelectedVersion() {
    if (!selectedCase || !selectedRow || !selectedAnswerVersion) {
      return;
    }
    try {
      setError(null);
      await approveRow(
        selectedCase.id,
        selectedRow.id,
        selectedAnswerVersion.id,
      );
      setStatus(`Approved version ${selectedAnswerVersion.version_number}.`);
      await refreshSelectedCase(selectedCase.id, selectedRow.id);
    } catch (caught) {
      setError((caught as Error).message);
    }
  }

  async function handleRejectRow() {
    if (!selectedCase || !selectedRow || !selectedAnswerVersion) {
      return;
    }
    try {
      setError(null);
      await rejectRow(
        selectedCase.id,
        selectedRow.id,
        selectedAnswerVersion.id,
      );
      setStatus("Row marked as rejected.");
      await refreshSelectedCase(selectedCase.id, selectedRow.id);
    } catch (caught) {
      setError((caught as Error).message);
    }
  }

  async function handleGenerateAnswer() {
    if (!selectedCase || !selectedRow || isDrafting) {
      return;
    }
    const answerThreadId = inspectedAnswerVersion
      ? threadLabelForAnswerVersion(
          inspectedAnswerVersion,
          threadState?.thread.id ?? null,
        )
      : undefined;
    try {
      setError(null);
      setIsDrafting(true);
      setStatus("Waiting for model response...");
      const result = await draftAnswer(
        selectedCase.id,
        selectedRow.id,
        inspectedAnswerVersion
          ? "Regenerate the answer with the latest grounded evidence."
          : "Generate a grounded answer for this row.",
        inspectedAnswerVersion ? answerThreadId : undefined,
        inspectedAnswerVersion ? "content_change" : undefined,
      );
      setThreadState(threadDetailFromDraftResponse(result));
      setSelectedAnswerVersionId(result.answer_version.id);
      setDraftActionMode(null);
      setStatus(
        inspectedAnswerVersion ? "Answer regenerated." : "Answer generated.",
      );
      await refreshSelectedCase(selectedCase.id, selectedRow.id);
    } catch (caught) {
      setError((caught as Error).message);
      setStatus(
        inspectedAnswerVersion ? "Regeneration failed." : "Generation failed.",
      );
    } finally {
      setIsDrafting(false);
    }
  }

  function summaryCount(
    source: Record<string, unknown> | undefined,
    key: string,
  ): number {
    const value = source?.[key];
    return typeof value === "number" ? value : 0;
  }

  function rowExecutionCounts() {
    const summary =
      (selectedCase?.latest_bulk_fill?.summary["row_execution_counts"] as
        | Record<string, unknown>
        | undefined) ?? undefined;
    return summary;
  }

  function reviewCounts() {
    const summary =
      (selectedCase?.latest_bulk_fill?.summary["review_status_counts"] as
        | Record<string, unknown>
        | undefined) ?? undefined;
    return summary;
  }

  return (
    <div
      className={
        isSidebarCollapsed ? "app-shell sidebar-collapsed" : "app-shell"
      }
    >
      <aside className={isSidebarCollapsed ? "sidebar collapsed" : "sidebar"}>
        <button
          type="button"
          className="sidebar-edge-toggle"
          onClick={() => setIsSidebarCollapsed((current) => !current)}
          aria-label={
            isSidebarCollapsed
              ? "Expand RfX RAG Expert sidebar"
              : "Collapse RfX RAG Expert sidebar"
          }
        >
          <SidebarEdgeToggleIcon collapsed={isSidebarCollapsed} />
        </button>
        {isSidebarCollapsed ? (
          <div className="panel-collapsed-shell">
            <span className="collapsed-rail-label">RfX RAG Expert</span>
          </div>
        ) : (
          <>
            <div className="sidebar-header">
              <div className="sidebar-header-main">
                <p className="eyebrow">RfX RAG Expert</p>
                <h1>Case workspace</h1>
                <p>
                  {session
                    ? `${session.user_name} in ${session.tenant_name}`
                    : status}
                </p>
                <div className="sidebar-header-actions">
                  <a
                    className="panel-toggle sidebar-toggle"
                    href="/?page=code-model-help"
                  >
                    Help
                  </a>
                </div>
              </div>
              <div className="sidebar-header-controls">
                <ThemeToggle
                  className="sidebar-theme-toggle"
                  preference={preference}
                  onChange={setPreference}
                />
              </div>
            </div>

            {devPanelsEnabled ? (
              <section className="tool-card">
                <div className="list-header">
                  <h2>Developer tools</h2>
                  <span>Enabled</span>
                </div>
                <div className="view-switch">
                  <button
                    type="button"
                    className={
                      activeViewMode === "workspace"
                        ? "mode-button active"
                        : "mode-button"
                    }
                    onClick={() => setViewMode("workspace")}
                  >
                    Workspace
                  </button>
                  <button
                    type="button"
                    className={
                      activeViewMode === "data-browser"
                        ? "mode-button active"
                        : "mode-button"
                    }
                    onClick={() => setViewMode("data-browser")}
                  >
                    Data browser
                  </button>
                </div>
              </section>
            ) : null}

            <div className="case-list">
              <div className="list-header">
                <div>
                  <h2>Past cases</h2>
                  <p className="status-line">
                    Keep the active review workspace scoped to one case at a
                    time.
                  </p>
                </div>
                <div className="sidebar-list-actions">
                  <span>{cases.length}</span>
                  <button
                    type="button"
                    className="panel-toggle"
                    onClick={() => setIsCaseFormVisible((current) => !current)}
                  >
                    {caseFormVisible ? "Hide new case" : "New case"}
                  </button>
                </div>
              </div>
              {cases.map((item) => (
                <button
                  key={item.id}
                  className={
                    selectedCase?.id === item.id
                      ? "case-card active"
                      : "case-card"
                  }
                  onClick={() => {
                    void selectCase(item.id);
                  }}
                  type="button"
                >
                  <strong>{item.name}</strong>
                  <span>{item.client_name ?? "Unnamed client"}</span>
                  <small>{item.status}</small>
                </button>
              ))}
            </div>

            {caseFormVisible ? (
              <form className="case-form" onSubmit={handleCreateCase}>
                <div className="list-header">
                  <div>
                    <h2>Create case</h2>
                    <p className="status-line">
                      Start a separate case when you need a fresh evidence desk
                      and row queue.
                    </p>
                  </div>
                </div>
                <label>
                  <span>Case name</span>
                  <input
                    value={formState.name}
                    onChange={(event) =>
                      setFormState((current) => ({
                        ...current,
                        name: event.target.value,
                      }))
                    }
                    required
                  />
                </label>
                <label>
                  <span>Client name</span>
                  <input
                    value={formState.clientName}
                    onChange={(event) =>
                      setFormState((current) => ({
                        ...current,
                        clientName: event.target.value,
                      }))
                    }
                  />
                </label>
                <label>
                  <span>Client PDF</span>
                  <input
                    type="file"
                    accept=".pdf"
                    onChange={(event) =>
                      setFormState((current) => ({
                        ...current,
                        pdf: event.target.files?.[0] ?? null,
                      }))
                    }
                    required
                  />
                  {formState.pdf ? (
                    <span className="file-status">
                      PDF loaded: {formState.pdf.name}
                    </span>
                  ) : null}
                </label>
                <label>
                  <span>Questionnaire XLSX</span>
                  <input
                    type="file"
                    accept=".xlsx"
                    onChange={(event) =>
                      setFormState((current) => ({
                        ...current,
                        questionnaire: event.target.files?.[0] ?? null,
                      }))
                    }
                  />
                  {formState.questionnaire ? (
                    <span className="file-status">
                      Questionnaire loaded: {formState.questionnaire.name}
                    </span>
                  ) : null}
                </label>
                <button type="submit">Create case</button>
              </form>
            ) : null}
          </>
        )}
      </aside>

      {activeViewMode === "workspace" ? (
        <main
          ref={workspaceRef}
          className={
            isEvidenceCollapsed ? "workspace evidence-collapsed" : "workspace"
          }
          style={workspaceStyle}
        >
          <section className="panel case-panel queue-panel">
            <div className="case-overview">
              <div className="panel-header case-overview-header">
                <div>
                  <p className="eyebrow">Selected case</p>
                  <h2>{selectedCase?.name ?? "No case selected"}</h2>
                  <p className="status-line">
                    {selectedCase?.client_name
                      ? `${selectedCase.client_name} · ${humanizeStatus(selectedCase.status, "active")}`
                      : "Operational queue and export controls remain scoped to the selected case."}
                  </p>
                </div>
                <div className="action-row case-command-bar">
                  <button
                    type="button"
                    onClick={() => void handleExport("approved_only")}
                    disabled={!selectedCase}
                    title={exportApprovedTooltip()}
                  >
                    Export approved
                  </button>
                  <button
                    type="button"
                    className="ghost"
                    onClick={() => void handleExport("latest_available")}
                    disabled={!selectedCase}
                    title={exportLatestTooltip()}
                  >
                    Export latest
                  </button>
                  <button
                    type="button"
                    className="ghost"
                    onClick={() => void handleBulkFill()}
                    disabled={!selectedCase}
                    title={launchBulkFillTooltip()}
                  >
                    Launch bulk-fill
                  </button>
                </div>
              </div>
              <p className="status-line">{status}</p>
              {error ? <p className="error-banner">{error}</p> : null}

              {queueCounts ? (
                <div className="queue-summary-row">
                  <StatusChip
                    label={`${queueCounts.approved} approved`}
                    tone="success"
                    tooltip={queueSummaryTooltip(
                      "approved",
                      queueCounts.approved,
                    )}
                  />
                  <StatusChip
                    label={`${queueCounts.needsReview} needs review`}
                    tone="warning"
                    tooltip={queueSummaryTooltip(
                      "needs_review",
                      queueCounts.needsReview,
                    )}
                  />
                  <StatusChip
                    label={`${queueCounts.failed} failed`}
                    tone="danger"
                    tooltip={queueSummaryTooltip("failed", queueCounts.failed)}
                  />
                  <StatusChip
                    label={`${queueCounts.notStarted} not started`}
                    tone="neutral"
                    tooltip={queueSummaryTooltip(
                      "not_started",
                      queueCounts.notStarted,
                    )}
                  />
                </div>
              ) : null}

              {selectedCase?.latest_bulk_fill ? (
                <article className="case-ops-strip">
                  <div className="case-ops-strip-header">
                    <div>
                      <p className="eyebrow">Latest bulk-fill</p>
                      <h3>
                        {humanizeStatus(selectedCase.latest_bulk_fill.status)}
                      </h3>
                    </div>
                    <StatusChip
                      label={humanizeStatus(
                        selectedCase.latest_bulk_fill.status,
                      )}
                      tone={statusToneForBulkFill(
                        selectedCase.latest_bulk_fill.status,
                      )}
                      tooltip={bulkFillStatusTooltip(
                        selectedCase.latest_bulk_fill.status,
                      )}
                    />
                  </div>
                  <p className="status-line">
                    queued {summaryCount(rowExecutionCounts(), "not_started")} ·
                    running {summaryCount(rowExecutionCounts(), "running")} ·
                    drafted {summaryCount(rowExecutionCounts(), "drafted")} ·
                    failed {summaryCount(rowExecutionCounts(), "failed")}
                  </p>
                  <p className="status-line">
                    review {summaryCount(reviewCounts(), "needs_review")} ·
                    approved {summaryCount(reviewCounts(), "approved")} ·
                    rejected {summaryCount(reviewCounts(), "rejected")}
                  </p>
                  <div className="queue-chip-row">
                    <StatusChip
                      label={
                        selectedCase.latest_bulk_fill.execution_mode ??
                        "unclaimed"
                      }
                      tone="neutral"
                      muted
                      tooltip={bulkFillExecutionModeTooltip(
                        selectedCase.latest_bulk_fill.execution_mode,
                      )}
                    />
                    <StatusChip
                      label={
                        selectedCase.latest_bulk_fill.runner_id ?? "no runner"
                      }
                      tone="neutral"
                      muted
                      tooltip={bulkFillRunnerTooltip(
                        selectedCase.latest_bulk_fill.runner_id,
                      )}
                    />
                  </div>
                  <div className="action-row compact-actions">
                    <button
                      type="button"
                      className="ghost"
                      onClick={() => void handleRetryFailedBulkFill()}
                      disabled={
                        ![
                          "completed_with_failures",
                          "failed",
                          "cancelled",
                          "orphaned",
                        ].includes(selectedCase.latest_bulk_fill.status)
                      }
                    >
                      Retry failed
                    </button>
                    <button
                      type="button"
                      className="ghost"
                      onClick={() => void handleResumeBulkFill()}
                      disabled={
                        ![
                          "completed_with_failures",
                          "failed",
                          "cancelled",
                          "orphaned",
                        ].includes(selectedCase.latest_bulk_fill.status)
                      }
                    >
                      Resume
                    </button>
                    <button
                      type="button"
                      className="ghost"
                      onClick={() => void handleCancelBulkFill()}
                      disabled={
                        !["queued", "running"].includes(
                          selectedCase.latest_bulk_fill.status,
                        )
                      }
                    >
                      Cancel
                    </button>
                  </div>
                </article>
              ) : null}

              <article className="case-brief">
                <div className="case-brief-header">
                  <p className="eyebrow">Case profile</p>
                  {!selectedCase?.profile ? (
                    <StatusChip
                      label="profile missing"
                      tone="warning"
                      tooltip={profileMissingTooltip()}
                    />
                  ) : null}
                </div>
                <p
                  className={
                    selectedCase?.profile && !isCaseProfileExpanded
                      ? "case-brief-summary clamped"
                      : "case-brief-summary"
                  }
                >
                  {selectedCase?.profile?.summary ??
                    "No case_profile is available yet for this case."}
                </p>
                {selectedCase?.profile?.summary ? (
                  <button
                    type="button"
                    className="question-context-toggle"
                    onClick={() =>
                      setIsCaseProfileExpanded((current) => !current)
                    }
                  >
                    {isCaseProfileExpanded ? "Show less" : "Show summary"}
                  </button>
                ) : null}
              </article>
            </div>

            <div className="row-list">
              <div className="queue-header">
                <div>
                  <h3>Questionnaire rows</h3>
                  <p className="status-line">
                    {filteredRows.length} rows in queue. Scan state first, then
                    open one row at a time.
                  </p>
                </div>
                <div className="queue-toolbar">
                  <label className="filter-row">
                    <span>Filter by state</span>
                    <select
                      value={rowFilter}
                      onChange={(event) =>
                        setRowFilter(event.target.value as RowFilter)
                      }
                    >
                      <option value="all">all</option>
                      <option value="not_started">not started</option>
                      <option value="running">running</option>
                      <option value="needs_review">needs review</option>
                      <option value="approved">approved</option>
                      <option value="rejected">rejected</option>
                      <option value="failed">failed</option>
                      <option value="skipped">skipped</option>
                    </select>
                  </label>
                </div>
              </div>
              <div className="queue-list">
                {filteredRows.map((row) => {
                  const answerStateLabel =
                    row.latest_attempt_state === "answer_available" ||
                    row.review_status === "approved" ||
                    row.review_status === "needs_review"
                      ? "answer ready"
                      : row.latest_attempt_state === "pending_no_answer" ||
                          row.review_status === "running"
                        ? "in progress"
                        : row.latest_attempt_state === "failed_no_answer" ||
                            row.review_status === "failed"
                          ? "failed attempt"
                          : "not started";
                  const secondaryState = row.approved_answer_version_id
                    ? "approved pointer"
                    : row.last_bulk_fill_status
                      ? humanizeStatus(row.last_bulk_fill_status)
                      : null;
                  return (
                    <button
                      key={row.id}
                      type="button"
                      aria-label={rowQueueAriaLabel(row)}
                      title={row.source_row_id}
                      data-row-visual-state={rowVisualState(row)}
                      className={
                        selectedRow?.id === row.id
                          ? "row-card active"
                          : "row-card"
                      }
                      onClick={() => {
                        if (selectedCase) {
                          void selectRow(selectedCase, row);
                        }
                      }}
                    >
                      <div className="row-card-top">
                        <strong>Row {row.source_row_number}</strong>
                        <div className="queue-chip-row">
                          <StatusChip
                            label={reviewStatusLabel(row.review_status, {
                              hasApprovedAnswer:
                                !!row.approved_answer_version_id ||
                                !!row.approved_answer_text,
                            })}
                            tone={statusToneForReviewStatus(row.review_status)}
                            tooltip={reviewStatusTooltip(row.review_status, {
                              hasApprovedAnswer:
                                !!row.approved_answer_version_id ||
                                !!row.approved_answer_text,
                            })}
                          />
                        </div>
                      </div>
                      <p className="row-card-question">{row.question}</p>
                      <div className="row-card-meta-line">
                        <span>{answerStateLabel}</span>
                        {secondaryState ? <span>{secondaryState}</span> : null}
                        {row.last_bulk_fill_attempt_number ? (
                          <span>
                            attempt {row.last_bulk_fill_attempt_number}
                          </span>
                        ) : null}
                      </div>
                    </button>
                  );
                })}
              </div>
            </div>
          </section>

          <section
            ref={answerPanelRef}
            className={
              isChatFocusMode
                ? "panel answer-panel chat-focus-mode"
                : "panel answer-panel"
            }
          >
            <article
              className={
                hasGeneratedAnswer || hasApprovedAnswer
                  ? "workspace-hero compact"
                  : "workspace-hero"
              }
            >
              <div className="workspace-hero-main">
                <div className="workspace-hero-kicker">
                  <div className="workspace-hero-copy">
                    <p className="eyebrow">Selected row</p>
                    <div className="workspace-hero-id-row">
                      <h2 className="workspace-hero-row-label">
                        {selectedRow
                          ? `Row ${selectedRow.source_row_number}`
                          : "Select a row"}
                      </h2>
                    </div>
                    <p className="workspace-hero-question">
                      {selectedRow?.question ??
                        "Select a row from the queue to inspect its drafting lineage."}
                    </p>
                    <div className="workspace-inline-context">
                    <span title={rowBackgroundTooltip()}>Row background</span>
                    <p
                      className={
                        isContextExpanded
                          ? "workspace-context-copy"
                          : "workspace-context-copy clamped"
                        }
                      >
                        {selectedRow?.context ?? "No row background loaded."}
                      </p>
                      {selectedRow && (selectedRow.context.length ?? 0) > 180 ? (
                        <button
                          type="button"
                          className="question-context-toggle"
                          onClick={() =>
                            setIsContextExpanded((current) => !current)
                          }
                        >
                          {isContextExpanded
                            ? "Show less background"
                            : "Show full background"}
                        </button>
                      ) : null}
                    </div>
                  </div>
                  <div className="workspace-hero-status-rail">
                    {selectedRow ? (
                      <StatusChip
                        label={reviewStatusLabel(selectedRow.review_status, {
                          hasApprovedAnswer,
                        })}
                        tone={statusToneForReviewStatus(
                          selectedRow.review_status,
                        )}
                        tooltip={reviewStatusTooltip(selectedRow.review_status, {
                          hasApprovedAnswer,
                        })}
                      />
                    ) : null}
                    <StatusChip
                      label={humanizeStatus(retrievalSufficiency, "not run")}
                      tone={statusToneForSufficiency(retrievalSufficiency)}
                      tooltip={retrievalSufficiencyTooltip(
                        retrievalSufficiency,
                      )}
                    />
                    <StatusChip
                      label={`Attempt ${attemptStateLabel(activeAttemptState)}`}
                      tone={statusToneForAttemptState(activeAttemptState)}
                      tooltip={attemptStateTooltip(activeAttemptState)}
                    />
                    <StatusChip
                      label={
                        hasApprovedAnswer
                          ? selectedAnswerIsApproved
                            ? "viewing approved answer"
                            : "approved answer retained"
                          : "Approval not set"
                      }
                      tone={
                        hasApprovedAnswer
                          ? selectedAnswerIsApproved
                            ? "success"
                            : "warning"
                          : "neutral"
                      }
                      tooltip={approvalStateTooltip({
                        hasApprovedAnswer,
                        selectedAnswerIsApproved,
                      })}
                    />
                    {selectedRow?.last_bulk_fill_status ? (
                      <StatusChip
                        label={`${humanizeStatus(selectedRow.last_bulk_fill_status)} · attempt ${selectedRow.last_bulk_fill_attempt_number}`}
                        tone={statusToneForBulkFill(
                          selectedRow.last_bulk_fill_status,
                        )}
                        muted
                        tooltip={rowBulkFillAttemptTooltip(
                          selectedRow.last_bulk_fill_status,
                          selectedRow.last_bulk_fill_attempt_number,
                        )}
                      />
                    ) : null}
                  </div>
                </div>
                {!hasGeneratedAnswer ? (
                  <div className="workspace-next-step compact">
                    <span>Next step</span>
                    <p>{nextStepSummary}</p>
                  </div>
                ) : null}
                {!hasGeneratedAnswer && !hasApprovedAnswer ? (
                  <div className="row-meta-grid workspace-meta-grid">
                    <div className="row-meta-item">
                      <span>Latest attempt</span>
                      <strong>{attemptStateLabel(activeAttemptState)}</strong>
                    </div>
                    <div className="row-meta-item">
                      <span>Retrieval sufficiency</span>
                      <strong>
                        {humanizeStatus(retrievalSufficiency, "not run")}
                      </strong>
                    </div>
                    <div className="row-meta-item">
                      <span>Approved answer</span>
                      <strong>not approved</strong>
                    </div>
                  </div>
                ) : null}
              </div>
            </article>

            <div className="answer-toolbar">
              <div
                className="answer-view-tabs"
                role="tablist"
                aria-label="Review view"
              >
                <button
                  type="button"
                  className={
                    reviewViewMode === "draft" ? "view-tab active" : "view-tab"
                  }
                  onClick={() => setReviewViewMode("draft")}
                  aria-pressed={reviewViewMode === "draft"}
                >
                  Read draft
                </button>
                <button
                  type="button"
                  className={
                    reviewViewMode === "compare"
                      ? "view-tab active"
                      : "view-tab"
                  }
                  onClick={() => setReviewViewMode("compare")}
                  aria-pressed={reviewViewMode === "compare"}
                  disabled={!hasApprovedAnswer || !selectedAnswerVersion}
                >
                  Compare
                </button>
                <button
                  type="button"
                  className={
                    reviewViewMode === "history"
                      ? "view-tab active"
                      : "view-tab"
                  }
                  onClick={() => setReviewViewMode("history")}
                  aria-pressed={reviewViewMode === "history"}
                  disabled={answerVersions.length < 1}
                >
                  History
                </button>
              </div>
            </div>

            <div
              ref={chatLogRef}
              className="chat-log answer-stage"
              aria-label="Conversation history"
              style={
                chatViewportHeight
                  ? {
                      height: `${chatViewportHeight}px`,
                      minHeight: `${chatViewportHeight}px`,
                      maxHeight: `${chatViewportHeight}px`,
                    }
                  : undefined
              }
            >
              {reviewViewMode === "draft" ? (
                <>
                  <div className="timeline-header">
                    <div>
                      <p className="eyebrow">Conversation</p>
                      <h3>Thread history</h3>
                    </div>
                    <p className="status-line">
                      The conversation is the primary review artifact for this
                      row. Read the latest assistant output in sequence with the
                      revision history.
                    </p>
                  </div>

                  {threadState?.thread_state === "failed_no_answer" ? (
                    <article className="timeline-failure">
                      <div className="timeline-entry-header">
                        <span className="timeline-entry-eyebrow">
                          Latest draft attempt failed
                        </span>
                        <StatusChip
                          label={attemptStateLabel(threadState.thread_state)}
                          tone="danger"
                          tooltip={attemptStateTooltip(threadState.thread_state)}
                        />
                      </div>
                      <p>
                        {threadState.failure_detail ??
                          "The latest attempt did not produce an answer version."}
                      </p>
                    </article>
                  ) : null}

                  {(threadState?.messages ?? []).length ? (
                    (() => {
                      let assistantIndex = 0;
                      let userIndex = 0;
                      return (threadState?.messages ?? []).map(
                        (item, index) => {
                          const messageVisualState: ChatMessageVisualState =
                            approvedMessageIndex < 0
                              ? "neutral"
                              : index === approvedMessageIndex
                                ? "approved"
                                : index > approvedMessageIndex
                                  ? "after-approved"
                                  : "neutral";
                          if (item.role === "assistant") {
                            assistantIndex += 1;
                          }
                          if (item.role === "user") {
                            userIndex += 1;
                          }
                          const linkedVersion = item.answer_version_id
                            ? answerVersionsById.get(item.answer_version_id)
                            : null;
                          return (
                            <article
                              key={item.id}
                              data-message-visual-state={messageVisualState}
                              className="message timeline-entry"
                              data-role={item.role}
                            >
                              <div className="timeline-marker" />
                              <div className="timeline-entry-body">
                                <div className="timeline-entry-header">
                                  <span className="timeline-entry-eyebrow">
                                    {timelineEntryEyebrow(item)}
                                  </span>
                                  <div className="queue-chip-row">
                                    {linkedVersion ? (
                                      <StatusChip
                                        label={`v${linkedVersion.version_number}`}
                                        tone="accent"
                                        muted
                                        tooltip={answerVersionTooltip(
                                          linkedVersion.version_number,
                                        )}
                                      />
                                    ) : null}
                                    {item.answer_version_id ===
                                    selectedRow?.approved_answer_version_id ? (
                                      <StatusChip
                                        label="approved pointer"
                                        tone="success"
                                        muted
                                        tooltip={approvedPointerTooltip()}
                                      />
                                    ) : null}
                                  </div>
                                </div>
                                <h4>
                                  {timelineEntryTitle(
                                    item,
                                    assistantIndex - 1,
                                    userIndex - 1,
                                  )}
                                </h4>
                                <p>{item.content}</p>
                              </div>
                            </article>
                          );
                        },
                      );
                    })()
                  ) : (
                    <article className="timeline-empty">
                      <h4>No drafting events yet</h4>
                      <p>
                        Generate or retry a grounded answer to start the row
                        timeline.
                      </p>
                    </article>
                  )}
                  {hasApprovedAnswer && !selectedAnswerIsApproved ? (
                    <article className="conversation-callout">
                      <span>
                        The currently approved answer is older than the selected
                        draft.
                      </span>
                      <button
                        type="button"
                        className="panel-toggle"
                        onClick={() => setReviewViewMode("compare")}
                      >
                        Compare with approved
                      </button>
                    </article>
                  ) : null}
                </>
              ) : null}

              {reviewViewMode === "compare" ? (
                <div className="compare-mode-layout">
                  <article className="answer-output-card compare-card">
                    <div className="answer-output-header">
                      <div>
                        <p className="eyebrow">Selected version</p>
                        <h3>
                          {selectedAnswerVersion
                            ? `Version ${selectedAnswerVersion.version_number}`
                            : "No selected version"}
                        </h3>
                      </div>
                      {selectedAnswerVersion ? (
                        <StatusChip
                          label={generationPathLabel(
                            selectedAnswerVersion.generation_path,
                          )}
                          tone="accent"
                          muted
                          tooltip={generationPathTooltip(
                            selectedAnswerVersion.generation_path,
                          )}
                        />
                      ) : null}
                    </div>
                    <p className="answer-output-text">
                      {selectedAnswerVersion?.answer_text ??
                        "Select a version from history to compare it."}
                    </p>
                  </article>
                  <article className="answer-output-card compare-card approved-card">
                    <div className="answer-output-header">
                      <div>
                        <p className="eyebrow">Approved answer</p>
                        <h3>
                          {approvedAnswerVersion
                            ? `Version ${approvedAnswerVersion.version_number}`
                            : "No approved answer"}
                        </h3>
                      </div>
                      {approvedAnswerVersion ? (
                        <StatusChip
                          label="canonical"
                          tone="success"
                          muted
                          tooltip={canonicalTooltip()}
                        />
                      ) : null}
                    </div>
                    <p className="answer-output-text">
                      {approvedAnswerVersion?.answer_text ??
                        selectedRow?.approved_answer_text ??
                        "No approved answer yet."}
                    </p>
                  </article>
                </div>
              ) : null}

              {reviewViewMode === "history" ? (
                <div className="history-mode-layout">
                  <div className="history-list-header">
                    <div>
                      <p className="eyebrow">Answer history</p>
                      <h3>Version list</h3>
                    </div>
                    <p className="status-line">
                      Select a version to inspect it, then approve or reject
                      that version directly.
                    </p>
                  </div>
                  <div className="history-rail history-rail-wide">
                    {answerVersions.map((version) => (
                      <button
                        key={version.id}
                        type="button"
                        aria-label={`Version ${version.version_number}`}
                        className={
                          selectedAnswerVersionId === version.id
                            ? "history-item active"
                            : "history-item"
                        }
                        onClick={() => void inspectAnswerVersion(version)}
                      >
                        <div className="history-item-top">
                          <strong>Version {version.version_number}</strong>
                          <div className="queue-chip-row">
                            <StatusChip
                              label={generationPathLabel(
                                version.generation_path,
                              )}
                              tone="neutral"
                              muted
                              tooltip={generationPathTooltip(
                                version.generation_path,
                              )}
                            />
                            {version.id ===
                            selectedRow?.approved_answer_version_id ? (
                              <StatusChip
                                label="approved pointer"
                                tone="success"
                                muted
                                tooltip={approvedPointerTooltip()}
                              />
                            ) : null}
                          </div>
                        </div>
                        <p
                          className={
                            selectedAnswerVersionId === version.id
                              ? "history-answer-text"
                              : "history-answer-text clamped"
                          }
                        >
                          {version.answer_text}
                        </p>
                      </button>
                    ))}
                  </div>
                </div>
              ) : null}
            </div>

            <div ref={composerRef} className="review-action-bar">
              {reviewViewMode === "draft" && draftActionMode === "revise" ? (
                <div className="composer review-drawer">
                  <label className="revision-brief">
                    <span>Revision brief</span>
                    <textarea
                      value={message}
                      onChange={(event) => setMessage(event.target.value)}
                      placeholder="Ask for a style revision such as shorter, clearer, or more formal."
                      rows={4}
                      disabled={isDrafting}
                    />
                  </label>
                  <div className="action-row">
                    <button
                      type="button"
                      onClick={() => void handleReviseAnswer()}
                      disabled={!selectedRow || !message.trim() || isDrafting}
                    >
                      {isDrafting
                        ? "Waiting for model response..."
                        : "Revise answer"}
                    </button>
                    <button
                      type="button"
                      className="ghost"
                      onClick={() => setDraftActionMode(null)}
                      disabled={isDrafting}
                    >
                      Cancel
                    </button>
                  </div>
                </div>
              ) : null}

              {reviewViewMode === "draft" &&
              draftActionMode === "regenerate" ? (
                <div className="composer review-drawer">
                  <div className="review-drawer-copy">
                    <span>Content refresh</span>
                    <p>
                      Regeneration reruns retrieval and may change grounded
                      claims. Use this when the answer needs new substance, not
                      just cleaner wording.
                    </p>
                  </div>
                  <div className="action-row">
                    <button
                      type="button"
                      onClick={() => void handleGenerateAnswer()}
                      disabled={!selectedRow || isDrafting}
                    >
                      {isDrafting
                        ? "Waiting for model response..."
                        : "Regenerate answer"}
                    </button>
                    <button
                      type="button"
                      className="ghost"
                      onClick={() => setDraftActionMode(null)}
                      disabled={isDrafting}
                    >
                      Cancel
                    </button>
                  </div>
                </div>
              ) : null}

              <div className="action-row primary-review-actions">
                {hasGeneratedAnswer && reviewViewMode === "draft" ? (
                  <>
                    <button
                      type="button"
                      className={
                        draftActionMode === "revise"
                          ? "ghost active-toggle"
                          : "ghost"
                      }
                      onClick={() =>
                        setDraftActionMode((current) =>
                          current === "revise" ? null : "revise",
                        )
                      }
                      disabled={!selectedRow || isDrafting}
                    >
                      Revise wording
                    </button>
                    <button
                      type="button"
                      className={
                        draftActionMode === "regenerate"
                          ? "ghost active-toggle"
                          : "ghost"
                      }
                      onClick={() =>
                        setDraftActionMode((current) =>
                          current === "regenerate" ? null : "regenerate",
                        )
                      }
                      disabled={!selectedRow || isDrafting}
                    >
                      Regenerate content
                    </button>
                    <button
                      type="button"
                      className="ghost"
                      onClick={() => void handleApproveSelectedVersion()}
                      disabled={!selectedRow || !selectedAnswerVersion}
                    >
                      Approve selected version
                    </button>
                    <button
                      type="button"
                      className="ghost"
                      onClick={() => void handleRejectRow()}
                      disabled={!selectedRow || !selectedAnswerVersion}
                    >
                      Reject row
                    </button>
                  </>
                ) : hasGeneratedAnswer &&
                  (reviewViewMode === "history" ||
                    reviewViewMode === "compare") ? (
                  <div className="history-review-actions">
                    <p className="status-line">
                      {rejectActionHint({
                        selectedVersionId: selectedAnswerVersion?.id ?? null,
                        approvedVersionId:
                          selectedRow?.approved_answer_version_id,
                        hasApprovedAnswer,
                      })}
                    </p>
                    <div className="action-row">
                      <button
                        type="button"
                        className="ghost"
                        onClick={() => void handleApproveSelectedVersion()}
                        disabled={!selectedRow || !selectedAnswerVersion}
                      >
                        Approve selected version
                      </button>
                      <button
                        type="button"
                        className="ghost"
                        onClick={() => void handleRejectRow()}
                        disabled={!selectedRow || !selectedAnswerVersion}
                      >
                        {rejectActionLabel({
                          selectedVersionId: selectedAnswerVersion?.id ?? null,
                          approvedVersionId:
                            selectedRow?.approved_answer_version_id,
                          hasApprovedAnswer,
                        })}
                      </button>
                    </div>
                  </div>
                ) : (
                  <>
                    <button
                      type="button"
                      onClick={() => void handleGenerateAnswer()}
                      disabled={!selectedRow || isDrafting}
                    >
                      {isDrafting
                        ? "Waiting for model response..."
                        : activeAttemptState === "failed_no_answer"
                          ? "Retry answer"
                          : "Generate answer"}
                    </button>
                    <button type="button" className="ghost" disabled>
                      Approve selected version
                    </button>
                    <button type="button" className="ghost" disabled>
                      Reject row
                    </button>
                  </>
                )}
              </div>
            </div>

            {devPanelsEnabled ? (
              <article className="dev-card">
                <div className="list-header">
                  <h3>Render-stage LLM payload</h3>
                  <span>
                    {inspectedAnswerVersion
                      ? `v${inspectedAnswerVersion.version_number}`
                      : "none"}
                  </span>
                </div>
                {inspectedAnswerVersion ? (
                  <>
                    <p className="status-line">
                      {generationPathLabel(
                        inspectedAnswerVersion.generation_path,
                      )}{" "}
                      ·{" "}
                      {inspectedAnswerVersion.llm_capture_stage ??
                        "no captured stage"}{" "}
                      · {inspectedAnswerVersion.llm_capture_status}
                    </p>
                    {inspectedAnswerVersion.llm_capture_status ===
                    "captured" ? (
                      <>
                        <label className="raw-block">
                          <span>Raw render prompt sent to LLM</span>
                          <textarea
                            readOnly
                            value={
                              inspectedAnswerVersion.llm_request_text ?? ""
                            }
                            rows={12}
                          />
                        </label>
                        <label className="raw-block">
                          <span>Raw render-stage model response</span>
                          <textarea
                            readOnly
                            value={
                              inspectedAnswerVersion.llm_response_text ?? ""
                            }
                            rows={10}
                          />
                        </label>
                        <p className="empty-dev-state">
                          Planning-stage lineage is stored separately in model
                          invocations and execution runs.
                        </p>
                      </>
                    ) : (
                      <p className="empty-dev-state">
                        Render-stage prompt capture is unavailable for this
                        answer version.
                      </p>
                    )}
                  </>
                ) : (
                  <p className="empty-dev-state">No answer version selected.</p>
                )}
              </article>
            ) : null}
          </section>

          <section
            className={
              isEvidenceCollapsed
                ? "panel evidence-panel collapsed"
                : "panel evidence-panel"
            }
            data-resizing={isEvidenceResizing ? "true" : "false"}
          >
            <button
              type="button"
              className="evidence-edge-toggle"
              onClick={() => setIsEvidenceCollapsed((current) => !current)}
              aria-label={
                isEvidenceCollapsed
                  ? "Expand retrieved evidence panel"
                  : "Collapse retrieved evidence panel"
              }
            >
              <SidebarEdgeToggleIcon
                collapsed={isEvidenceCollapsed}
                side="right"
              />
            </button>
            {!isEvidenceCollapsed ? (
              <div
                className={
                  isEvidenceResizing
                    ? "evidence-panel-resizer active"
                    : "evidence-panel-resizer"
                }
                role="separator"
                tabIndex={0}
                aria-label="Resize evidence inspector"
                aria-orientation="vertical"
                aria-valuemin={EVIDENCE_PANEL_MIN_WIDTH}
                aria-valuemax={evidencePanelMaxWidth}
                aria-valuenow={effectiveEvidencePanelWidth}
                onPointerDown={handleEvidenceResizeStart}
                onKeyDown={handleEvidenceResizeKeyDown}
              />
            ) : null}
            {isEvidenceCollapsed ? (
              <div className="panel-collapsed-shell">
                <span className="collapsed-rail-label">Retrieved evidence</span>
              </div>
            ) : (
              <>
                <div className="panel-header">
                  <div>
                    <p className="eyebrow">Evidence inspector</p>
                    <h2>Grounding</h2>
                  </div>
                </div>
                <div className="evidence-panel-body">
                  {threadState?.retrieval ? (
                    <div className="retrieval-summary inspector-summary">
                      <div className="queue-chip-row">
                        <StatusChip
                          label={humanizeStatus(
                            threadState.retrieval.sufficiency,
                          )}
                          tone={statusToneForSufficiency(
                            threadState.retrieval.sufficiency,
                          )}
                          tooltip={retrievalSufficiencyTooltip(
                            threadState.retrieval.sufficiency,
                          )}
                        />
                        <StatusChip
                          label={retrievalActionLabel(
                            threadState.retrieval.retrieval_action,
                          )}
                          tone="accent"
                          muted
                          tooltip={retrievalActionTooltip(
                            threadState.retrieval.retrieval_action,
                          )}
                        />
                        <StatusChip
                          label={revisionModeLabel(
                            threadState.retrieval.revision_mode,
                          )}
                          tone="neutral"
                          muted
                          tooltip={revisionModeTooltip(
                            threadState.retrieval.revision_mode,
                          )}
                        />
                        {threadState.retrieval.broadened ? (
                          <StatusChip
                            label="broadened"
                            tone="warning"
                            muted
                            tooltip={broadenedTooltip()}
                          />
                        ) : null}
                        {threadState.retrieval.degraded ? (
                          <StatusChip
                            label="degraded"
                            tone="warning"
                            muted
                            tooltip={degradedTooltip()}
                          />
                        ) : null}
                      </div>
                      {threadState.retrieval.notes.length ? (
                        <ul className="retrieval-note-list">
                          {threadState.retrieval.notes.map((note) => (
                            <li key={note}>{note}</li>
                          ))}
                        </ul>
                      ) : null}
                    </div>
                  ) : null}

                  {evidenceAuthorityGroups.length ? (
                    evidenceAuthorityGroups.map(({ meta, items }) => (
                      <article
                        key={meta.key}
                        className="evidence-group"
                        data-authority={meta.key}
                      >
                        <div className="evidence-group-header">
                          <div className="evidence-group-title-row">
                            <h3>{meta.label}</h3>
                            <span className="evidence-group-count">
                              {pluralize(items.length, "item", "items")}
                            </span>
                          </div>
                          <p className="status-line">{meta.description}</p>
                        </div>
                        {items.map((item) => {
                          const detailRows = evidenceDetails(item);
                          const historicalSections =
                            item.source_kind === "historical_qa_row"
                              ? parseHistoricalEvidenceExcerpt(item.excerpt)
                              : null;
                          const isExpanded = expandedEvidenceIds.includes(
                            item.id,
                          );
                          return (
                            <div
                              key={item.id}
                              className="evidence-card"
                              data-authority={meta.key}
                            >
                              <div className="evidence-card-header">
                                <div className="evidence-card-title-block">
                                  <p className="evidence-card-kicker">
                                    {evidenceTraceLabel(item)}
                                  </p>
                                  <h4 className="evidence-card-title">
                                    {evidenceDisplayTitle(item)}
                                  </h4>
                                </div>
                                <MetaBadge
                                  label={`Relevance ${item.score.toFixed(3)}`}
                                  tooltip={relevanceTooltip(item.score)}
                                />
                              </div>
                              {historicalSections ? (
                                <div className="historical-evidence">
                                  {historicalSections.map((section) => (
                                    <section
                                      key={`${item.id}-${section.label}`}
                                      className="historical-evidence-section"
                                    >
                                      <p className="historical-evidence-label">
                                        {section.label}
                                      </p>
                                      <p
                                        className={
                                          isExpanded
                                            ? "historical-evidence-copy"
                                            : "historical-evidence-copy clamped"
                                        }
                                      >
                                        {section.value}
                                      </p>
                                    </section>
                                  ))}
                                </div>
                              ) : (
                                <p
                                  className={
                                    isExpanded
                                      ? "evidence-excerpt"
                                      : "evidence-excerpt clamped"
                                  }
                                >
                                  {formatEvidenceExcerpt(item)}
                                </p>
                              )}
                              {item.excerpt.length > 180 ? (
                                <button
                                  type="button"
                                  className="question-context-toggle evidence-toggle"
                                  onClick={() =>
                                    setExpandedEvidenceIds((current) =>
                                      current.includes(item.id)
                                        ? current.filter(
                                            (value) => value !== item.id,
                                          )
                                        : [...current, item.id],
                                    )
                                  }
                                >
                                  {isExpanded
                                    ? "Show less"
                                    : historicalSections
                                      ? "Show full example"
                                      : "Show excerpt"}
                                </button>
                              ) : null}
                              {detailRows.length ? (
                                <dl className="evidence-meta-list">
                                  {detailRows.map((detail) => (
                                    <div
                                      key={`${item.id}-${detail.label}`}
                                      className="evidence-meta-row"
                                    >
                                      <dt>{detail.label}</dt>
                                      <dd>{detail.value}</dd>
                                    </div>
                                  ))}
                                </dl>
                              ) : null}
                            </div>
                          );
                        })}
                      </article>
                    ))
                  ) : (
                    <article className="evidence-group empty">
                      <h3>No evidence yet</h3>
                      <p>
                        Retrieval evidence stays separate from the drafting
                        timeline. Generate or inspect a row to populate this
                        inspector.
                      </p>
                    </article>
                  )}
                </div>
              </>
            )}
          </section>
        </main>
      ) : (
        <main className="workspace data-browser-layout">
          <section className="panel data-browser-panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Developer tools</p>
                <h2>Backend tables</h2>
              </div>
            </div>
            <p className="status-line">
              {selectedCase && useSelectedCaseFilter
                ? `Showing case-scoped rows for ${selectedCase.name} where supported.`
                : "Showing tenant-scoped rows."}
            </p>
            {error ? <p className="error-banner">{error}</p> : null}
            <div className="dev-toolbar">
              <label className="checkbox-row">
                <input
                  type="checkbox"
                  checked={useSelectedCaseFilter}
                  disabled={!selectedCase}
                  onChange={(event) =>
                    setUseSelectedCaseFilter(event.target.checked)
                  }
                />
                <span>Filter to selected case where supported</span>
              </label>
            </div>
            <div className="data-browser-grid">
              <article className="dev-table-list">
                <div className="list-header">
                  <h3>Tables</h3>
                  <span>{devTables.length}</span>
                </div>
                {devTables.map((table) => (
                  <button
                    key={table.name}
                    type="button"
                    className={
                      selectedDevTable === table.name
                        ? "table-card active"
                        : "table-card"
                    }
                    onClick={() => setSelectedDevTable(table.name)}
                  >
                    <strong>{table.name}</strong>
                    <span>{table.row_count} rows</span>
                    <small>
                      {table.case_filter_supported
                        ? "case-filter ready"
                        : "tenant scope only"}
                    </small>
                  </button>
                ))}
              </article>

              <article className="dev-table-view">
                <div className="list-header">
                  <h3>{selectedDevTable ?? "Select a table"}</h3>
                  <span>{devTableRows?.row_count ?? 0}</span>
                </div>
                {isDevLoading ? (
                  <p className="status-line">Loading table rows...</p>
                ) : devTableRows ? (
                  <>
                    <p className="status-line">
                      {devTableRows.case_filter_applied
                        ? "Case filter applied."
                        : "Tenant scope only."}
                    </p>
                    <div className="dev-table-wrapper">
                      <table className="dev-table">
                        <thead>
                          <tr>
                            {devTableRows.columns.map((column) => (
                              <th key={column}>{column}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {devTableRows.rows.map((row, index) => (
                            <tr key={`${devTableRows.table_name}-${index}`}>
                              {devTableRows.columns.map((column) => (
                                <td key={`${index}-${column}`}>
                                  <pre>{renderDevValue(row[column])}</pre>
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </>
                ) : (
                  <p className="empty-dev-state">
                    Select a table to browse rows.
                  </p>
                )}
              </article>
            </div>
          </section>
        </main>
      )}
    </div>
  );
}

export default App;
