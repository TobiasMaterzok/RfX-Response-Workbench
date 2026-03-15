export type SessionContext = {
  tenant_id: string;
  tenant_slug: string;
  tenant_name: string;
  user_id: string;
  user_email: string;
  user_name: string;
};

export type CaseSummary = {
  id: string;
  name: string;
  client_name: string | null;
  language: string;
  status: string;
  created_at: string;
  updated_at: string;
};

export type QuestionnaireRow = {
  id: string;
  source_row_id: string;
  source_row_number: number;
  context: string;
  question: string;
  current_answer: string;
  review_status: string;
  approved_answer_version_id: string | null;
  approved_answer_text: string | null;
  last_error_detail: string | null;
  last_bulk_fill_request_id: string | null;
  last_bulk_fill_row_execution_id: string | null;
  last_bulk_fill_status: string | null;
  last_bulk_fill_attempt_number: number | null;
  latest_attempt_thread_id: string | null;
  latest_attempt_state:
    | "none"
    | "answer_available"
    | "failed_no_answer"
    | "pending_no_answer";
};

export type ChatThread = {
  id: string;
  questionnaire_row_id: string;
  title: string;
  updated_at: string;
};

export type CaseProfileSummary = {
  schema_version: string;
  prompt_set_version: string;
  summary: string;
  generated_at: string;
};

export type CaseDetail = {
  id: string;
  name: string;
  client_name: string | null;
  language: string;
  status: string;
  created_at: string;
  updated_at: string;
  profile: CaseProfileSummary | null;
  latest_bulk_fill: BulkFillSummary | null;
  bulk_fill_history: BulkFillSummary[];
  questionnaire_rows: QuestionnaireRow[];
  chats: ChatThread[];
};

export type Evidence = {
  id: string;
  source_kind:
    | "case_profile_item"
    | "historical_qa_row"
    | "product_truth_chunk"
    | "pdf_chunk"
    | "pdf_page";
  source_label: string;
  source_title: string;
  excerpt: string;
  score: number;
  metadata: Record<string, unknown>;
};

export type RetrievalStage = {
  corpus: string;
  stage: string;
  mode: string;
  candidate_count: number;
  broadened: boolean;
  skipped: boolean;
  reason: string | null;
};

export type RetrievalSummary = {
  strategy_version: string;
  pipeline_profile_name: string | null;
  pipeline_config_hash: string;
  index_config_hash: string;
  revision_mode: string;
  revision_classifier_version: string | null;
  revision_reason: string | null;
  retrieval_action: string;
  retrieval_action_reason: string | null;
  reused_from_retrieval_run_id: string | null;
  candidate_generation_mode: string;
  broadened: boolean;
  sufficiency: string;
  degraded: boolean;
  notes: string[];
  stages: RetrievalStage[];
};

export type AnswerVersion = {
  id: string;
  chat_thread_id: string;
  retrieval_run_id: string;
  version_number: number;
  answer_text: string;
  status: string;
  pipeline_profile_name: string | null;
  pipeline_config_hash: string;
  created_at: string;
  model: string;
  generation_path: string;
  llm_capture_stage: string | null;
  prompt_version: string;
  llm_capture_status: string;
  llm_request_text: string | null;
  llm_response_text: string | null;
};

export type ChatMessage = {
  id: string;
  role: string;
  content: string;
  created_at: string;
  answer_version_id?: string | null;
};

export type DraftResponse = {
  thread: ChatThread;
  messages: ChatMessage[];
  answer_version: AnswerVersion;
  retrieval: RetrievalSummary;
  evidence: Evidence[];
};

export type ThreadDetail = {
  thread: ChatThread;
  thread_state:
    | "none"
    | "answer_available"
    | "failed_no_answer"
    | "pending_no_answer";
  messages: ChatMessage[];
  answer_version: AnswerVersion | null;
  retrieval: RetrievalSummary | null;
  evidence: Evidence[];
  failure_detail: string | null;
};

export type RawTraceScope = "selected_answer_version" | "latest_attempt";

export type RawTraceStage = {
  availability: "available" | "missing";
  source_type: string | null;
  source_execution_run_id: string | null;
  source_answer_version_id: string | null;
  model_invocation_id: string | null;
  prompt_family: string | null;
  prompt_version: string | null;
  requested_model_id: string | null;
  actual_model_id: string | null;
  reasoning_effort: string | null;
  temperature: number | null;
  provider_response_id: string | null;
  service_tier: string | null;
  usage_json: Record<string, unknown> | null;
  request_payload_text: string | null;
  response_payload_text: string | null;
};

export type RawTrace = {
  scope: RawTraceScope;
  row_id: string;
  thread_id: string | null;
  execution_run_id: string | null;
  answer_version_id: string | null;
  generation_path: string | null;
  latest_attempt_state:
    | "none"
    | "answer_available"
    | "failed_no_answer"
    | "pending_no_answer";
  failure_detail: string | null;
  planning_stage: RawTraceStage;
  rendering_stage: RawTraceStage;
};

export type ExportResponse = {
  export_job_id: string;
  status: string;
  export_mode: string;
  includes_unapproved_drafts: boolean;
  placeholder_row_count: number;
  download_upload_id: string;
  csv_download_upload_id: string;
  zip_download_upload_id: string;
};

export type BulkFillSummary = {
  id: string;
  parent_request_id: string | null;
  status: string;
  created_at: string;
  updated_at: string;
  claim_id: string | null;
  runner_id: string | null;
  execution_mode: string | null;
  claimed_at: string | null;
  started_at: string | null;
  heartbeat_at: string | null;
  finished_at: string | null;
  cancel_requested_at: string | null;
  stale_detected_at: string | null;
  summary: Record<string, unknown>;
  error_detail: string | null;
  config: Record<string, unknown>;
};

export type BulkFillResponse = {
  request: BulkFillSummary;
};

export type DevTableSummary = {
  name: string;
  row_count: number;
  case_filter_supported: boolean;
};

export type DevTableListResponse = {
  tables: DevTableSummary[];
};

export type DevTableRowsResponse = {
  table_name: string;
  row_count: number;
  case_filter_applied: boolean;
  columns: string[];
  rows: Record<string, unknown>[];
};
