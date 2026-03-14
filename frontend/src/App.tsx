import {
  FormEvent,
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
  DevTableRowsResponse,
  DevTableSummary,
  DraftResponse,
  Evidence,
  QuestionnaireRow,
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

const emptyCaseState: CreateCaseState = {
  name: "",
  clientName: "",
  pdf: null,
  questionnaire: null,
};

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
  const [isContextExpanded, setIsContextExpanded] = useState(false);
  const [chatViewportHeight, setChatViewportHeight] = useState<number | null>(
    null,
  );
  const activeViewMode: ViewMode = devPanelsEnabled ? viewMode : "workspace";
  const workspaceLoadIdRef = useRef(0);
  const answerPanelRef = useRef<HTMLElement | null>(null);
  const chatLogRef = useRef<HTMLDivElement | null>(null);
  const composerRef = useRef<HTMLDivElement | null>(null);

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

  const groupedEvidence = (threadState?.evidence ?? []).reduce<
    Record<string, Evidence[]>
  >((accumulator, item) => {
    const key = item.source_label;
    const current = accumulator[key] ?? [];
    current.push(item);
    accumulator[key] = current;
    return accumulator;
  }, {});

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
  const lastBulkFillAttemptLabel = selectedRow?.last_bulk_fill_status
    ? `${selectedRow.last_bulk_fill_status} · attempt ${selectedRow.last_bulk_fill_attempt_number}`
    : "No bulk-fill attempt recorded.";
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

  useLayoutEffect(() => {
    function updateChatViewportHeight() {
      if (
        activeViewMode !== "workspace" ||
        !answerPanelRef.current ||
        !chatLogRef.current ||
        !composerRef.current ||
        window.innerWidth <= 1100
      ) {
        setChatViewportHeight(null);
        return;
      }
      const panelTop = Math.max(
        answerPanelRef.current.getBoundingClientRect().top,
        24,
      );
      const chatTop = panelTop + chatLogRef.current.offsetTop;
      const composerHeight = composerRef.current.getBoundingClientRect().height;
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
    hasGeneratedAnswer,
    isContextExpanded,
    isEvidenceCollapsed,
    isSidebarCollapsed,
    latestChatMessageId,
    selectedRow?.id,
  ]);

  async function selectCase(caseId: string) {
    try {
      setError(null);
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
        {isSidebarCollapsed ? (
          <div className="panel-collapsed-shell">
            <button
              type="button"
              className="panel-toggle sidebar-toggle"
              onClick={() => setIsSidebarCollapsed(false)}
              aria-label="Expand RfX RAG Expert sidebar"
            >
              Show
            </button>
            <span className="collapsed-rail-label">RfX RAG Expert</span>
          </div>
        ) : (
          <>
            <div className="sidebar-header">
              <div>
                <p className="eyebrow">RfX RAG Expert</p>
                <h1>Case workspace</h1>
                <p>
                  {session
                    ? `${session.user_name} in ${session.tenant_name}`
                    : status}
                </p>
              </div>
              <div className="sidebar-header-controls">
                <div className="sidebar-header-actions">
                  <a
                    className="panel-toggle sidebar-toggle"
                    href="/?page=code-model-help"
                  >
                    Help
                  </a>
                  <button
                    type="button"
                    className="panel-toggle sidebar-toggle"
                    onClick={() => setIsSidebarCollapsed(true)}
                    aria-label="Collapse RfX RAG Expert sidebar"
                  >
                    Hide
                  </button>
                </div>
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

            <form className="case-form" onSubmit={handleCreateCase}>
              <h2>Create case</h2>
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

            <div className="case-list">
              <div className="list-header">
                <h2>Past cases</h2>
                <span>{cases.length}</span>
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
          </>
        )}
      </aside>

      {activeViewMode === "workspace" ? (
        <main
          className={
            isEvidenceCollapsed ? "workspace evidence-collapsed" : "workspace"
          }
        >
          <section className="panel case-panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Selected case</p>
                <h2>{selectedCase?.name ?? "No case selected"}</h2>
              </div>
              <div className="action-row">
                <button
                  type="button"
                  onClick={() => void handleExport("approved_only")}
                  disabled={!selectedCase}
                >
                  Export approved
                </button>
                <button
                  type="button"
                  className="ghost"
                  onClick={() => void handleExport("latest_available")}
                  disabled={!selectedCase}
                >
                  Export latest
                </button>
                <button
                  type="button"
                  className="ghost"
                  onClick={() => void handleBulkFill()}
                  disabled={!selectedCase}
                >
                  Launch bulk-fill
                </button>
              </div>
            </div>
            <p className="status-line">{status}</p>
            {error ? <p className="error-banner">{error}</p> : null}
            {selectedCase?.latest_bulk_fill ? (
              <article className="profile-card">
                <div className="list-header">
                  <h3>Latest bulk-fill</h3>
                  <span>{selectedCase.latest_bulk_fill.status}</span>
                </div>
                <p>
                  queued {summaryCount(rowExecutionCounts(), "not_started")} ·
                  running {summaryCount(rowExecutionCounts(), "running")} ·
                  drafted {summaryCount(rowExecutionCounts(), "drafted")} ·
                  failed {summaryCount(rowExecutionCounts(), "failed")}
                </p>
                <p>
                  review {summaryCount(reviewCounts(), "needs_review")} ·
                  approved {summaryCount(reviewCounts(), "approved")} · rejected{" "}
                  {summaryCount(reviewCounts(), "rejected")}
                </p>
                <p>
                  {selectedCase.latest_bulk_fill.execution_mode ?? "unclaimed"}{" "}
                  · {selectedCase.latest_bulk_fill.runner_id ?? "no runner"}
                </p>
                {selectedCase.latest_bulk_fill.cancel_requested_at ? (
                  <p>
                    Cancel requested at{" "}
                    {selectedCase.latest_bulk_fill.cancel_requested_at}
                  </p>
                ) : null}
                {selectedCase.latest_bulk_fill.stale_detected_at ? (
                  <p>
                    Orphaned/stale at{" "}
                    {selectedCase.latest_bulk_fill.stale_detected_at}
                  </p>
                ) : null}
                <div className="action-row">
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
            {selectedCase?.bulk_fill_history?.length ? (
              <article className="profile-card">
                <div className="list-header">
                  <h3>Bulk-fill history</h3>
                  <span>{selectedCase.bulk_fill_history.length}</span>
                </div>
                {selectedCase.bulk_fill_history.map((job) => (
                  <div key={job.id} className="history-item">
                    <strong>{job.status}</strong>
                    <p>
                      {job.execution_mode ?? "unclaimed"} ·{" "}
                      {job.runner_id ?? "no runner"}
                    </p>
                    <small>{job.created_at}</small>
                  </div>
                ))}
              </article>
            ) : null}
            {selectedCase?.profile ? (
              <article className="profile-card">
                <h3>Case profile</h3>
                <p>{selectedCase.profile.summary}</p>
                <small>
                  {selectedCase.profile.schema_version} ·{" "}
                  {selectedCase.profile.prompt_set_version}
                </small>
              </article>
            ) : (
              <article className="profile-card empty">
                <h3>Case profile</h3>
                <p>No case_profile is available.</p>
              </article>
            )}

            <div className="row-list">
              <div className="list-header">
                <h3>Questionnaire rows</h3>
                <span>{filteredRows.length}</span>
              </div>
              <label className="filter-row">
                <span>Filter</span>
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
              {filteredRows.map((row) => (
                <button
                  key={row.id}
                  type="button"
                  data-row-visual-state={rowVisualState(row)}
                  className={
                    selectedRow?.id === row.id ? "row-card active" : "row-card"
                  }
                  onClick={() => {
                    if (selectedCase) {
                      void selectRow(selectedCase, row);
                    }
                  }}
                >
                  <strong>Row {row.source_row_number}</strong>
                  <span>{row.question}</span>
                  <small>{row.review_status}</small>
                  {row.last_bulk_fill_status ? (
                    <small>
                      {row.last_bulk_fill_status} · attempt{" "}
                      {row.last_bulk_fill_attempt_number}
                    </small>
                  ) : null}
                </button>
              ))}
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
            <div className="panel-header">
              <div>
                <p className="eyebrow">Draft workspace</p>
                <h2>
                  {selectedRow
                    ? `Row ${selectedRow.source_row_number}`
                    : "Select a row"}
                </h2>
              </div>
            </div>
            <article className="question-card">
              <div className="question-copy">
                <div>
                  <h3>Question</h3>
                  <p>
                    {selectedRow?.question ?? "No questionnaire row selected."}
                  </p>
                </div>
                <div className="question-context-block">
                  <h3>Context</h3>
                  <p
                    className={
                      hasLongContext && !isContextExpanded
                        ? "question-context-preview clamped"
                        : "question-context-preview"
                    }
                  >
                    {selectedRow?.context ?? "No row context loaded."}
                  </p>
                  {hasLongContext ? (
                    <button
                      type="button"
                      className="question-context-toggle"
                      onClick={() =>
                        setIsContextExpanded((current) => !current)
                      }
                    >
                      {isContextExpanded ? "Show less" : "Show full context"}
                    </button>
                  ) : null}
                </div>
              </div>
              <div className="row-meta-grid">
                <div className="row-meta-item">
                  <span>Review</span>
                  <strong>
                    {selectedRow?.review_status ?? "No row selected."}
                  </strong>
                </div>
                <div className="row-meta-item">
                  <span>Latest attempt</span>
                  <strong>{attemptStateLabel(activeAttemptState)}</strong>
                </div>
                <div className="row-meta-item">
                  <span>Bulk-fill</span>
                  <strong>{lastBulkFillAttemptLabel}</strong>
                </div>
              </div>
            </article>

            <div
              ref={chatLogRef}
              className="chat-log"
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
              {(threadState?.messages ?? []).map((item, index) => {
                const messageVisualState: ChatMessageVisualState =
                  approvedMessageIndex < 0
                    ? "neutral"
                    : index === approvedMessageIndex
                      ? "approved"
                      : index > approvedMessageIndex
                        ? "after-approved"
                        : "neutral";
                return (
                  <article
                    key={item.id}
                    data-message-visual-state={messageVisualState}
                    className={
                      item.role === "assistant"
                        ? "message assistant"
                        : "message"
                    }
                  >
                    <span>{item.role}</span>
                    <p>{item.content}</p>
                  </article>
                );
              })}
            </div>

            <div ref={composerRef} className="composer">
              {hasGeneratedAnswer ? (
                <>
                  <textarea
                    value={message}
                    onChange={(event) => setMessage(event.target.value)}
                    placeholder="Ask for a style revision such as shorter, clearer, or more formal."
                    rows={5}
                    disabled={isDrafting}
                  />
                  <button
                    type="button"
                    onClick={() => void handleReviseAnswer()}
                    disabled={!selectedRow || !message.trim() || isDrafting}
                  >
                    {isDrafting
                      ? "Waiting for model response..."
                      : "Revise answer"}
                  </button>
                  <div className="action-row">
                    <button
                      type="button"
                      className="ghost"
                      onClick={() => void handleGenerateAnswer()}
                      disabled={!selectedRow || isDrafting}
                    >
                      Regenerate answer
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
                  </div>
                </>
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
                  <div className="action-row">
                    <button type="button" className="ghost" disabled>
                      Approve selected version
                    </button>
                    <button type="button" className="ghost" disabled>
                      Reject row
                    </button>
                  </div>
                </>
              )}
            </div>

            {threadState?.thread_state === "failed_no_answer" ? (
              <article className="draft-card">
                <div className="list-header">
                  <h3>Latest draft attempt failed</h3>
                  <span>{attemptStateLabel(threadState.thread_state)}</span>
                </div>
                <p>
                  {threadState.failure_detail ??
                    "The latest attempt did not produce an answer version."}
                </p>
              </article>
            ) : null}

            <article className="draft-card">
              <div className="list-header">
                <h3>Latest drafted answer</h3>
                <span>
                  {selectedAnswerVersion
                    ? `v${selectedAnswerVersion.version_number}`
                    : "none"}
                </span>
              </div>
              <p>
                {selectedAnswerVersion?.answer_text ??
                  (threadState?.thread_state === "failed_no_answer"
                    ? "The latest attempt failed before producing an answer version. Select an earlier version from history or retry."
                    : "No draft yet.")}
              </p>
            </article>

            <article className="draft-card">
              <div className="list-header">
                <h3>Approved answer</h3>
                <span>
                  {approvedAnswerVersion
                    ? `v${approvedAnswerVersion.version_number}`
                    : "none"}
                </span>
              </div>
              <p>
                {approvedAnswerVersion?.answer_text ??
                  selectedRow?.approved_answer_text ??
                  "No approved answer yet."}
              </p>
            </article>

            <article className="history-card">
              <div className="list-header">
                <h3>Answer history</h3>
                <span>{answerVersions.length}</span>
              </div>
              {answerVersions.map((version) => (
                <button
                  key={version.id}
                  type="button"
                  className={
                    selectedAnswerVersionId === version.id
                      ? "history-item active"
                      : "history-item"
                  }
                  onClick={() => void inspectAnswerVersion(version)}
                >
                  <strong>Version {version.version_number}</strong>
                  <p>{version.answer_text}</p>
                </button>
              ))}
            </article>

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
                      {inspectedAnswerVersion.generation_path.replaceAll(
                        "_",
                        " ",
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
          >
            {isEvidenceCollapsed ? (
              <div className="panel-collapsed-shell">
                <button
                  type="button"
                  className="panel-toggle"
                  onClick={() => setIsEvidenceCollapsed(false)}
                  aria-label="Expand retrieved evidence panel"
                >
                  Show
                </button>
                <span className="collapsed-rail-label">Retrieved evidence</span>
              </div>
            ) : (
              <>
                <div className="panel-header">
                  <div>
                    <p className="eyebrow">Retrieved evidence</p>
                    <h2>Grounding</h2>
                  </div>
                  <button
                    type="button"
                    className="panel-toggle"
                    onClick={() => setIsEvidenceCollapsed(true)}
                    aria-label="Collapse retrieved evidence panel"
                  >
                    Hide
                  </button>
                </div>
                {threadState?.retrieval ? (
                  <div className="retrieval-summary">
                    <p className="status-line">
                      {threadState.retrieval.retrieval_action.replaceAll(
                        "_",
                        " ",
                      )}{" "}
                      ·{" "}
                      {threadState.retrieval.revision_mode.replaceAll("_", " ")}{" "}
                      · {threadState.retrieval.sufficiency}
                      {threadState.retrieval.broadened ? " · broadened" : ""}
                      {threadState.retrieval.degraded ? " · degraded" : ""}
                    </p>
                    {threadState.retrieval.notes.length ? (
                      <ul className="retrieval-note-list">
                        {threadState.retrieval.notes.map((note) => (
                          <li key={note}>{note}</li>
                        ))}
                      </ul>
                    ) : null}
                  </div>
                ) : null}

                {Object.entries(groupedEvidence).length ? (
                  Object.entries(groupedEvidence).map(([label, items]) => (
                    <article key={label} className="evidence-group">
                      <h3>{label.replaceAll("_", " ")}</h3>
                      {items.map((item) => (
                        <div key={item.id} className="evidence-card">
                          <strong>{item.source_title}</strong>
                          <p>{item.excerpt}</p>
                          <small>
                            {item.source_kind} · score {item.score.toFixed(3)}
                          </small>
                        </div>
                      ))}
                    </article>
                  ))
                ) : (
                  <article className="evidence-group empty">
                    <h3>No evidence yet</h3>
                    <p>
                      The answer panel stays separate from retrieved evidence.
                      Draft a row to populate this panel.
                    </p>
                  </article>
                )}
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
