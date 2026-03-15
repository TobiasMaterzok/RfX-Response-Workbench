import type {
  AnswerVersion,
  CaseDetail,
  CaseSummary,
  DevTableListResponse,
  DevTableRowsResponse,
  DraftResponse,
  BulkFillResponse,
  ExportResponse,
  RawTrace,
  RawTraceScope,
  SessionContext,
  ThreadDetail,
} from "../types";

const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:8000";
const TENANT_SLUG = import.meta.env.VITE_TENANT_SLUG ?? "local-workspace";
const USER_EMAIL = import.meta.env.VITE_USER_EMAIL ?? "local.user@example.test";

function buildHeaders(init?: HeadersInit): HeadersInit {
  return {
    "X-Tenant-Slug": TENANT_SLUG,
    "X-User-Email": USER_EMAIL,
    ...(init ?? {}),
  };
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    ...init,
    headers: buildHeaders(init?.headers),
  });
  if (!response.ok) {
    const body = (await response
      .json()
      .catch(() => ({ detail: response.statusText }))) as {
      detail?: string;
    };
    throw new Error(body.detail ?? response.statusText);
  }
  return (await response.json()) as T;
}

export async function getSessionContext(): Promise<SessionContext> {
  return request<SessionContext>("/api/session/context");
}

export async function listCases(): Promise<CaseSummary[]> {
  return request<CaseSummary[]>("/api/cases");
}

export async function getCaseDetail(caseId: string): Promise<CaseDetail> {
  return request<CaseDetail>(`/api/cases/${caseId}`);
}

export async function getThread(
  caseId: string,
  threadId: string,
): Promise<ThreadDetail> {
  return request<ThreadDetail>(`/api/cases/${caseId}/threads/${threadId}`);
}

export async function getRawTrace(
  caseId: string,
  rowId: string,
  options: {
    scope: RawTraceScope;
    answerVersionId?: string | null;
  },
): Promise<RawTrace> {
  const params = new URLSearchParams();
  params.set("scope", options.scope);
  if (options.answerVersionId) {
    params.set("answer_version_id", options.answerVersionId);
  }
  return request<RawTrace>(
    `/api/cases/${caseId}/rows/${rowId}/raw-trace?${params.toString()}`,
  );
}

export async function listAnswerVersions(
  caseId: string,
  rowId: string,
): Promise<AnswerVersion[]> {
  return request<AnswerVersion[]>(`/api/cases/${caseId}/rows/${rowId}/answers`);
}

export async function createCase(form: {
  name: string;
  clientName: string;
  pdf: File;
  questionnaire?: File | null;
}): Promise<CaseDetail> {
  const data = new FormData();
  data.append("name", form.name);
  data.append("client_name", form.clientName);
  data.append("pdf", form.pdf);
  if (form.questionnaire) {
    data.append("questionnaire", form.questionnaire);
  }
  return request<CaseDetail>("/api/cases", {
    method: "POST",
    body: data,
  });
}

export async function draftAnswer(
  caseId: string,
  rowId: string,
  message: string,
  threadId?: string,
  revisionModeOverride?: "style_only" | "content_change",
): Promise<DraftResponse> {
  return request<DraftResponse>(`/api/cases/${caseId}/rows/${rowId}/draft`, {
    method: "POST",
    body: JSON.stringify({
      message,
      thread_id: threadId ?? null,
      revision_mode_override: revisionModeOverride ?? null,
    }),
    headers: {
      "Content-Type": "application/json",
    },
  });
}

export async function exportCase(
  caseId: string,
  mode: "approved_only" | "latest_available",
): Promise<ExportResponse> {
  return request<ExportResponse>(`/api/cases/${caseId}/export`, {
    method: "POST",
    body: JSON.stringify({ mode }),
    headers: {
      "Content-Type": "application/json",
    },
  });
}

function buildDownloadUrl(uploadId: string): string {
  return `${API_BASE_URL}/api/cases/downloads/${uploadId}`;
}

function parseDownloadFileName(
  contentDisposition: string | null,
  fallbackName: string,
): string {
  if (!contentDisposition) {
    return fallbackName;
  }
  const match = contentDisposition.match(/filename="?([^"]+)"?/i);
  return match?.[1] ?? fallbackName;
}

function fallbackDownloadFileName(
  uploadId: string,
  mediaType: string | null,
): string {
  if (mediaType === "application/zip") {
    return `${uploadId}.zip`;
  }
  if (mediaType === "text/csv") {
    return `${uploadId}.csv`;
  }
  return `${uploadId}.xlsx`;
}

export async function downloadUpload(uploadId: string): Promise<void> {
  const response = await fetch(buildDownloadUrl(uploadId), {
    headers: buildHeaders(),
  });
  if (!response.ok) {
    const body = (await response
      .json()
      .catch(() => ({ detail: response.statusText }))) as {
      detail?: string;
    };
    throw new Error(body.detail ?? response.statusText);
  }
  const blob = await response.blob();
  const downloadUrl = URL.createObjectURL(blob);
  const fileName = parseDownloadFileName(
    response.headers.get("content-disposition"),
    fallbackDownloadFileName(uploadId, response.headers.get("content-type")),
  );
  const anchor = document.createElement("a");
  anchor.href = downloadUrl;
  anchor.download = fileName;
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(downloadUrl);
}

export async function requestBulkFill(
  caseId: string,
): Promise<BulkFillResponse> {
  return request<BulkFillResponse>(`/api/cases/${caseId}/bulk-fill`, {
    method: "POST",
    body: JSON.stringify({ note: "First-cut bulk-fill placeholder" }),
    headers: {
      "Content-Type": "application/json",
    },
  });
}

export async function retryFailedBulkFill(
  caseId: string,
  requestId: string,
): Promise<BulkFillResponse> {
  return request<BulkFillResponse>(
    `/api/cases/${caseId}/bulk-fill/${requestId}/retry-failed`,
    {
      method: "POST",
    },
  );
}

export async function resumeBulkFill(
  caseId: string,
  requestId: string,
): Promise<BulkFillResponse> {
  return request<BulkFillResponse>(
    `/api/cases/${caseId}/bulk-fill/${requestId}/resume`,
    {
      method: "POST",
    },
  );
}

export async function cancelBulkFill(
  caseId: string,
  requestId: string,
): Promise<BulkFillResponse> {
  return request<BulkFillResponse>(
    `/api/cases/${caseId}/bulk-fill/${requestId}/cancel`,
    {
      method: "POST",
    },
  );
}

export async function approveRow(
  caseId: string,
  rowId: string,
  answerVersionId: string,
): Promise<CaseDetail["questionnaire_rows"][number]> {
  return request<CaseDetail["questionnaire_rows"][number]>(
    `/api/cases/${caseId}/rows/${rowId}/approve`,
    {
      method: "POST",
      body: JSON.stringify({ answer_version_id: answerVersionId }),
      headers: {
        "Content-Type": "application/json",
      },
    },
  );
}

export async function rejectRow(
  caseId: string,
  rowId: string,
  answerVersionId?: string,
): Promise<CaseDetail["questionnaire_rows"][number]> {
  return request<CaseDetail["questionnaire_rows"][number]>(
    `/api/cases/${caseId}/rows/${rowId}/reject`,
    {
      method: "POST",
      body: JSON.stringify({ answer_version_id: answerVersionId ?? null }),
      headers: {
        "Content-Type": "application/json",
      },
    },
  );
}

export async function listDevTables(): Promise<DevTableListResponse> {
  return request<DevTableListResponse>("/api/dev/tables");
}

export async function browseDevTable(
  tableName: string,
  options?: { caseId?: string; limit?: number },
): Promise<DevTableRowsResponse> {
  const params = new URLSearchParams();
  if (options?.caseId) {
    params.set("case_id", options.caseId);
  }
  if (options?.limit) {
    params.set("limit", String(options.limit));
  }
  const query = params.size ? `?${params.toString()}` : "";
  return request<DevTableRowsResponse>(`/api/dev/tables/${tableName}${query}`);
}
