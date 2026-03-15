import {
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";

import App from "./App";

const fetchMock = vi.fn();

describe("App", () => {
  beforeEach(() => {
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
    vi.unstubAllEnvs();
    window.history.replaceState({}, "", "/");
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.unstubAllEnvs();
    window.history.replaceState({}, "", "/");
  });

  it("shows selected file names in the create-case form", async () => {
    fetchMock.mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        const method = init?.method ?? "GET";

        if (url.endsWith("/api/session/context")) {
          return new Response(
            JSON.stringify({
              tenant_id: "tenant-1",
              tenant_slug: "local-workspace",
              tenant_name: "Local Workspace",
              user_id: "user-1",
              user_email: "local.user.test",
              user_name: "Local Admin",
            }),
          );
        }

        if (url.endsWith("/api/cases") && method === "GET") {
          return new Response(JSON.stringify([]));
        }

        return new Response(
          JSON.stringify({ detail: `Unhandled request ${method} ${url}` }),
          { status: 500 },
        );
      },
    );

    render(<App />);

    const pdfInput = await screen.findByLabelText("Client PDF");
    const questionnaireInput = screen.getByLabelText("Questionnaire XLSX");
    const pdfFile = new File(["pdf"], "client-brief.pdf", {
      type: "application/pdf",
    });
    const questionnaireFile = new File(["xlsx"], "questions.xlsx", {
      type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    });

    fireEvent.change(pdfInput, { target: { files: [pdfFile] } });
    fireEvent.change(questionnaireInput, {
      target: { files: [questionnaireFile] },
    });

    expect(
      screen.getByText("PDF loaded: client-brief.pdf"),
    ).toBeInTheDocument();
    expect(
      screen.getByText("Questionnaire loaded: questions.xlsx"),
    ).toBeInTheDocument();
  });

  it("renders evidence separately from the drafted answer and allows inspector resizing", async () => {
    const originalInnerWidth = window.innerWidth;
    Object.defineProperty(window, "innerWidth", {
      configurable: true,
      writable: true,
      value: 1440,
    });

    let drafted = false;
    fetchMock.mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        const method = init?.method ?? "GET";

        if (url.endsWith("/api/session/context")) {
          return new Response(
            JSON.stringify({
              tenant_id: "tenant-1",
              tenant_slug: "local-workspace",
              tenant_name: "Local Workspace",
              user_id: "user-1",
              user_email: "local.user.test",
              user_name: "Local Admin",
            }),
          );
        }

        if (url.endsWith("/api/cases") && method === "GET") {
          return new Response(
            JSON.stringify([
              {
                id: "case-1",
                name: "NordTransit Pilot",
                client_name: "NordTransit",
                language: "de",
                status: "active",
                created_at: "2026-03-06T10:00:00Z",
                updated_at: "2026-03-06T10:00:00Z",
              },
            ]),
          );
        }

        if (url.endsWith("/api/cases/case-1") && method === "GET") {
          return new Response(
            JSON.stringify({
              id: "case-1",
              name: "NordTransit Pilot",
              client_name: "NordTransit",
              language: "de",
              status: "active",
              created_at: "2026-03-06T10:00:00Z",
              updated_at: "2026-03-06T10:00:00Z",
              profile: {
                schema_version: "rfx_case_profile.v3",
                prompt_set_version: "rfx_case_profile_prompt_set.v3",
                summary: "Pilot summary",
                generated_at: "2026-03-06T10:00:00Z",
              },
              latest_bulk_fill: null,
              questionnaire_rows: [
                {
                  id: "row-1",
                  source_row_id: "workbook:QA:2",
                  source_row_number: 2,
                  context: "Shared context",
                  question: "What fits the scope?",
                  current_answer: drafted ? "Draft answer body" : "",
                  review_status: drafted ? "needs_review" : "not_started",
                  approved_answer_version_id: null,
                  approved_answer_text: null,
                  last_error_detail: null,
                },
              ],
              chats: drafted
                ? [
                    {
                      id: "thread-1",
                      questionnaire_row_id: "row-1",
                      title: "Row 2",
                      updated_at: "2026-03-06T10:10:00Z",
                    },
                  ]
                : [],
            }),
          );
        }

        if (
          url.endsWith("/api/cases/case-1/rows/row-1/answers") &&
          method === "GET"
        ) {
          return new Response(
            JSON.stringify(
              drafted
                ? [
                    {
                      id: "answer-1",
                      version_number: 1,
                      answer_text: "Draft answer body",
                      status: "draft",
                      created_at: "2026-03-06T10:10:00Z",
                      model: "stub-ai-service",
                      generation_path: "two_stage_plan_render",
                      llm_capture_stage: "answer_rendering",
                      prompt_version: "answer_rendering_prompt.v2",
                      llm_capture_status: "captured",
                      llm_request_text: "Prompt body",
                      llm_response_text: "Draft answer body",
                    },
                  ]
                : [],
            ),
          );
        }

        if (
          url.endsWith("/api/cases/case-1/rows/row-1/draft") &&
          method === "POST"
        ) {
          drafted = true;
          return new Response(
            JSON.stringify({
              thread: {
                id: "thread-1",
                questionnaire_row_id: "row-1",
                title: "Row 2",
                updated_at: "2026-03-06T10:10:00Z",
              },
              messages: [
                {
                  id: "message-1",
                  role: "assistant",
                  content: "Draft answer body",
                  created_at: "2026-03-06T10:10:00Z",
                },
              ],
              answer_version: {
                id: "answer-1",
                version_number: 1,
                answer_text: "Draft answer body",
                status: "draft",
                created_at: "2026-03-06T10:10:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v2",
                llm_capture_status: "captured",
                llm_request_text: "Prompt body",
                llm_response_text: "Draft answer body",
              },
              retrieval: {
                strategy_version: "retrieval.v2.hardened.v1",
                revision_mode: "initial_draft",
                revision_classifier_version: "revision_classifier.v2",
                revision_reason: "no_previous_answer_version",
                retrieval_action: "refresh_retrieval",
                retrieval_action_reason:
                  "new_or_content_change_requires_refresh",
                reused_from_retrieval_run_id: null,
                candidate_generation_mode: "sql_keyword_scope_python_rerank",
                broadened: false,
                sufficiency: "weak",
                degraded: false,
                notes: [],
                stages: [],
              },
              evidence: [
                {
                  id: "evidence-1",
                  source_kind: "case_profile_item",
                  source_label: "current_case_facts",
                  source_title: "initiative_scope",
                  excerpt: "Evidence excerpt",
                  score: 0.88,
                  metadata: {
                    confidence: "high",
                    citations: ["Page 1", "Page 2"],
                  },
                },
              ],
            }),
          );
        }

        if (
          url.endsWith("/api/cases/case-1/threads/thread-1") &&
          method === "GET"
        ) {
          return new Response(
            JSON.stringify({
              thread: {
                id: "thread-1",
                questionnaire_row_id: "row-1",
                title: "Row 2",
                updated_at: "2026-03-06T10:10:00Z",
              },
              messages: [
                {
                  id: "message-1",
                  role: "assistant",
                  content: "Draft answer body",
                  created_at: "2026-03-06T10:10:00Z",
                },
              ],
              answer_version: {
                id: "answer-1",
                version_number: 1,
                answer_text: "Draft answer body",
                status: "draft",
                created_at: "2026-03-06T10:10:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v2",
                llm_capture_status: "captured",
                llm_request_text: "Prompt body",
                llm_response_text: "Draft answer body",
              },
              retrieval: {
                strategy_version: "retrieval.v2.hardened.v1",
                revision_mode: "initial_draft",
                revision_classifier_version: "revision_classifier.v2",
                revision_reason: "no_previous_answer_version",
                retrieval_action: "refresh_retrieval",
                retrieval_action_reason:
                  "new_or_content_change_requires_refresh",
                reused_from_retrieval_run_id: null,
                candidate_generation_mode: "sql_keyword_scope_python_rerank",
                broadened: false,
                sufficiency: "weak",
                degraded: false,
                notes: [],
                stages: [],
              },
              evidence: [
                {
                  id: "evidence-1",
                  source_kind: "case_profile_item",
                  source_label: "current_case_facts",
                  source_title: "initiative_scope",
                  excerpt: "Evidence excerpt",
                  score: 0.88,
                  metadata: {
                    confidence: "high",
                    citations: ["Page 1", "Page 2"],
                  },
                },
              ],
            }),
          );
        }

        return new Response(
          JSON.stringify({ detail: `Unhandled request ${method} ${url}` }),
          {
            status: 500,
          },
        );
      },
    );

    try {
      render(<App />);

      await waitFor(() => {
        expect(screen.getByText("Questionnaire rows")).toBeInTheDocument();
      });

      fireEvent.click(screen.getByText("Generate answer"));

      await waitFor(() => {
        expect(screen.getAllByText("Draft answer body")[0]).toBeInTheDocument();
        expect(screen.getByText("Evidence excerpt")).toBeInTheDocument();
        expect(screen.getByText("Revise wording")).toBeInTheDocument();
      });

      expect(screen.getByText("Initiative scope")).toBeInTheDocument();
      expect(screen.getByText("Structured case fact")).toBeInTheDocument();
      expect(screen.getByText("Relevance 0.880")).toBeInTheDocument();
      expect(screen.getByText("Confidence")).toBeInTheDocument();
      expect(screen.getByText("High")).toBeInTheDocument();
      expect(screen.getByText("Page 1, Page 2")).toBeInTheDocument();
      expect(screen.getByText("0 approved")).toHaveAttribute(
        "title",
        expect.stringContaining("No rows currently have an approved answer"),
      );
      expect(screen.getAllByText("weak")[0]).toHaveAttribute(
        "title",
        expect.stringContaining("useful evidence"),
      );
      expect(screen.getByText("evidence refreshed")).toHaveAttribute(
        "title",
        expect.stringContaining("fresh evidence set"),
      );
      expect(screen.getByText("first draft")).toHaveAttribute(
        "title",
        expect.stringContaining("first drafting pass"),
      );
      expect(screen.getByText("Relevance 0.880")).toHaveAttribute(
        "title",
        expect.stringContaining("matched the selected row"),
      );
      expect(
        screen.getByRole("button", { name: "Export approved" }),
      ).toHaveAttribute("title", expect.stringContaining("approved answer"));
      expect(screen.getByRole("button", { name: "Export latest" })).toHaveAttribute(
        "title",
        expect.stringContaining("newest available answer"),
      );
      expect(
        screen.getByRole("button", { name: "Launch bulk-fill" }),
      ).toHaveAttribute("title", expect.stringContaining("case-wide drafting run"));

      const workspace = document.querySelector(".workspace");
      const separator = screen.getByRole("separator", {
        name: "Resize evidence inspector",
      });

      expect(workspace?.getAttribute("style")).toContain(
        "--workspace-right-column: 360px",
      );

      fireEvent.keyDown(separator, { key: "ArrowLeft" });

      await waitFor(() => {
        expect(separator).toHaveAttribute("aria-valuenow", "384");
      });
      expect(workspace?.getAttribute("style")).toContain(
        "--workspace-right-column: 384px",
      );

      fireEvent.click(screen.getByRole("button", { name: "Compare" }));

      await waitFor(() => {
        expect(
          screen.getByRole("button", {
            name: "Collapse retrieved evidence panel",
          }),
        ).toBeInTheDocument();
      });
      expect(
        screen.queryByRole("button", {
          name: "Expand retrieved evidence panel",
        }),
      ).not.toBeInTheDocument();

      expect(screen.queryByText("Data browser")).not.toBeInTheDocument();
      expect(screen.queryByDisplayValue("Prompt body")).not.toBeInTheDocument();
    } finally {
      Object.defineProperty(window, "innerWidth", {
        configurable: true,
        writable: true,
        value: originalInnerWidth,
      });
    }
  });

  it("shows historical examples first and preserves section breaks inside historical evidence", async () => {
    fetchMock.mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        const method = init?.method ?? "GET";

        if (url.endsWith("/api/session/context")) {
          return new Response(
            JSON.stringify({
              tenant_id: "tenant-1",
              tenant_slug: "local-workspace",
              tenant_name: "Local Workspace",
              user_id: "user-1",
              user_email: "local.user.test",
              user_name: "Local Admin",
            }),
          );
        }

        if (url.endsWith("/api/cases") && method === "GET") {
          return new Response(
            JSON.stringify([
              {
                id: "case-1",
                name: "NordTransit Pilot",
                client_name: "NordTransit",
                language: "de",
                status: "active",
                created_at: "2026-03-06T10:00:00Z",
                updated_at: "2026-03-06T10:00:00Z",
              },
            ]),
          );
        }

        if (url.endsWith("/api/cases/case-1") && method === "GET") {
          return new Response(
            JSON.stringify({
              id: "case-1",
              name: "NordTransit Pilot",
              client_name: "NordTransit",
              language: "de",
              status: "active",
              created_at: "2026-03-06T10:00:00Z",
              updated_at: "2026-03-06T10:00:00Z",
              profile: null,
              latest_bulk_fill: null,
              questionnaire_rows: [
                {
                  id: "row-1",
                  source_row_id: "workbook:QA:2",
                  source_row_number: 2,
                  context: "Shared context",
                  question: "What fits the scope?",
                  current_answer: "Draft answer body",
                  review_status: "needs_review",
                  approved_answer_version_id: null,
                  approved_answer_text: null,
                  last_error_detail: null,
                  last_bulk_fill_request_id: null,
                  last_bulk_fill_row_execution_id: null,
                  last_bulk_fill_status: null,
                  last_bulk_fill_attempt_number: null,
                  latest_attempt_thread_id: "thread-1",
                  latest_attempt_state: "answer_available",
                },
              ],
              chats: [
                {
                  id: "thread-1",
                  questionnaire_row_id: "row-1",
                  title: "Row 2",
                  updated_at: "2026-03-06T10:10:00Z",
                },
              ],
            }),
          );
        }

        if (
          url.endsWith("/api/cases/case-1/rows/row-1/answers") &&
          method === "GET"
        ) {
          return new Response(
            JSON.stringify([
              {
                id: "answer-1",
                chat_thread_id: "thread-1",
                version_number: 1,
                answer_text: "Draft answer body",
                status: "draft",
                created_at: "2026-03-06T10:10:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v2",
                llm_capture_status: "captured",
                llm_request_text: "Prompt body",
                llm_response_text: "Draft answer body",
              },
            ]),
          );
        }

        if (
          url.endsWith("/api/cases/case-1/threads/thread-1") &&
          method === "GET"
        ) {
          return new Response(
            JSON.stringify({
              thread: {
                id: "thread-1",
                questionnaire_row_id: "row-1",
                title: "Row 2",
                updated_at: "2026-03-06T10:10:00Z",
              },
              thread_state: "answer_available",
              messages: [],
              answer_version: {
                id: "answer-1",
                chat_thread_id: "thread-1",
                version_number: 1,
                answer_text: "Draft answer body",
                status: "draft",
                created_at: "2026-03-06T10:10:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v2",
                llm_capture_status: "captured",
                llm_request_text: "Prompt body",
                llm_response_text: "Draft answer body",
              },
              retrieval: {
                strategy_version: "retrieval.v2.hardened.v1",
                revision_mode: "initial_draft",
                revision_classifier_version: "revision_classifier.v2",
                revision_reason: "no_previous_answer_version",
                retrieval_action: "refresh_retrieval",
                retrieval_action_reason:
                  "new_or_content_change_requires_refresh",
                reused_from_retrieval_run_id: null,
                candidate_generation_mode: "sql_keyword_scope_python_rerank",
                broadened: false,
                sufficiency: "weak",
                degraded: false,
                notes: [],
                stages: [],
              },
              evidence: [
                {
                  id: "evidence-historical",
                  source_kind: "historical_qa_row",
                  source_label: "historical_exemplar",
                  source_title: "crownshield_insurance_services_ltd_qa.xlsx:QA:12",
                  excerpt: [
                    "Historical client context: CrownShield Insurance Services Ltd is a UK and Ireland specialty insurance services provider with a distributed broker network. The requested scope covers broker onboarding, policy servicing requests, low-complexity claims triage, and secure exchange of policy and claims documents. The main objective is to reduce shared-mailbox volume and create a consistent operational audit trail.",
                    "Historical question: How do you support a phased rollout across onboarding, servicing, and claims intake?",
                    "Historical answer exemplar: We typically start with one high-volume, low-complexity process such as broker onboarding or servicing requests, prove the operating model, and then extend the same platform to claims intake. Reusing roles, templates, notifications, and reporting reduces delivery risk and shortens each later phase.",
                  ].join("\n"),
                  score: 0.75,
                  metadata: {
                    provenance: {
                      client_slug: "crownshield_insurance_services_ltd",
                      source_row_number: 12,
                    },
                  },
                },
                {
                  id: "evidence-case",
                  source_kind: "case_profile_item",
                  source_label: "current_case_facts",
                  source_title: "initiative_scope",
                  excerpt: "Evidence excerpt",
                  score: 0.88,
                  metadata: {
                    confidence: "high",
                    citations: ["Page 1", "Page 2"],
                  },
                },
                {
                  id: "evidence-product",
                  source_kind: "product_truth_chunk",
                  source_label: "product_truth",
                  source_title: "BluePeak Vault feature note",
                  excerpt: "Product truth excerpt",
                  score: 0.65,
                  metadata: {},
                },
              ],
              failure_detail: null,
            }),
          );
        }

        return new Response(
          JSON.stringify({ detail: `Unhandled request ${method} ${url}` }),
          { status: 500 },
        );
      },
    );

    render(<App />);

    await waitFor(() => {
      expect(screen.getByText("Historical examples")).toBeInTheDocument();
    });

    const evidenceHeadings = Array.from(
      document.querySelectorAll(".evidence-group h3"),
    ).map((node) => node.textContent?.trim());

    expect(evidenceHeadings).toEqual([
      "Historical examples",
      "Current case facts",
      "Product truth",
    ]);
    expect(
      screen.getByText("Historical example · row 12"),
    ).toBeInTheDocument();
    expect(
      screen.getByText("Crownshield Insurance Services Ltd · Row 12"),
    ).toBeInTheDocument();
    expect(screen.getByText("Historical client context")).toBeInTheDocument();
    expect(screen.getByText("Historical question")).toBeInTheDocument();
    expect(screen.getByText("Historical answer exemplar")).toBeInTheDocument();
    expect(
      document.querySelectorAll(".historical-evidence-section"),
    ).toHaveLength(3);
  });

  it("shows raw planning and rendering traces and lets reviewers switch to the latest attempt scope", async () => {
    fetchMock.mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input), "http://localhost");
        const method = init?.method ?? "GET";

        if (url.pathname === "/api/session/context") {
          return new Response(
            JSON.stringify({
              tenant_id: "tenant-1",
              tenant_slug: "local-workspace",
              tenant_name: "Local Workspace",
              user_id: "user-1",
              user_email: "local.user.test",
              user_name: "Local Admin",
            }),
          );
        }

        if (url.pathname === "/api/cases" && method === "GET") {
          return new Response(
            JSON.stringify([
              {
                id: "case-1",
                name: "NordTransit Pilot",
                client_name: "NordTransit",
                language: "de",
                status: "active",
                created_at: "2026-03-06T10:00:00Z",
                updated_at: "2026-03-06T10:00:00Z",
              },
            ]),
          );
        }

        if (url.pathname === "/api/cases/case-1" && method === "GET") {
          return new Response(
            JSON.stringify({
              id: "case-1",
              name: "NordTransit Pilot",
              client_name: "NordTransit",
              language: "de",
              status: "active",
              created_at: "2026-03-06T10:00:00Z",
              updated_at: "2026-03-06T10:00:00Z",
              profile: null,
              latest_bulk_fill: null,
              bulk_fill_history: [],
              questionnaire_rows: [
                {
                  id: "row-1",
                  source_row_id: "workbook:QA:2",
                  source_row_number: 2,
                  context: "Shared context",
                  question: "What fits the scope?",
                  current_answer: "Latest draft answer",
                  review_status: "needs_review",
                  approved_answer_version_id: null,
                  approved_answer_text: null,
                  last_error_detail: null,
                  last_bulk_fill_request_id: null,
                  last_bulk_fill_row_execution_id: null,
                  last_bulk_fill_status: null,
                  last_bulk_fill_attempt_number: null,
                  latest_attempt_thread_id: "thread-2",
                  latest_attempt_state: "answer_available",
                },
              ],
              chats: [
                {
                  id: "thread-2",
                  questionnaire_row_id: "row-1",
                  title: "Row 2",
                  updated_at: "2026-03-06T10:20:00Z",
                },
                {
                  id: "thread-1",
                  questionnaire_row_id: "row-1",
                  title: "Row 2 older",
                  updated_at: "2026-03-06T10:10:00Z",
                },
              ],
            }),
          );
        }

        if (
          url.pathname === "/api/cases/case-1/rows/row-1/answers" &&
          method === "GET"
        ) {
          return new Response(
            JSON.stringify([
              {
                id: "answer-2",
                chat_thread_id: "thread-2",
                retrieval_run_id: "retrieval-2",
                version_number: 2,
                answer_text: "Latest draft answer",
                status: "draft",
                pipeline_profile_name: "default",
                pipeline_config_hash: "cfg-2",
                created_at: "2026-03-06T10:20:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v3",
                llm_capture_status: "captured",
                llm_request_text: "latest render request",
                llm_response_text: "Latest draft answer",
              },
              {
                id: "answer-1",
                chat_thread_id: "thread-1",
                retrieval_run_id: "retrieval-1",
                version_number: 1,
                answer_text: "Older approved-style draft",
                status: "draft",
                pipeline_profile_name: "default",
                pipeline_config_hash: "cfg-1",
                created_at: "2026-03-06T10:10:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v3",
                llm_capture_status: "captured",
                llm_request_text: "older render request",
                llm_response_text: "Older approved-style draft",
              },
            ]),
          );
        }

        if (
          url.pathname === "/api/cases/case-1/threads/thread-2" &&
          method === "GET"
        ) {
          return new Response(
            JSON.stringify({
              thread: {
                id: "thread-2",
                questionnaire_row_id: "row-1",
                title: "Row 2",
                updated_at: "2026-03-06T10:20:00Z",
              },
              thread_state: "answer_available",
              messages: [],
              answer_version: {
                id: "answer-1",
                chat_thread_id: "thread-2",
                retrieval_run_id: "retrieval-1",
                version_number: 1,
                answer_text: "Older approved-style draft",
                status: "draft",
                pipeline_profile_name: "default",
                pipeline_config_hash: "cfg-1",
                created_at: "2026-03-06T10:10:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v3",
                llm_capture_status: "captured",
                llm_request_text: "older render request",
                llm_response_text: "Older approved-style draft",
              },
              retrieval: null,
              evidence: [],
              failure_detail: null,
            }),
          );
        }

        if (
          url.pathname === "/api/cases/case-1/threads/thread-1" &&
          method === "GET"
        ) {
          return new Response(
            JSON.stringify({
              thread: {
                id: "thread-1",
                questionnaire_row_id: "row-1",
                title: "Row 2 older",
                updated_at: "2026-03-06T10:10:00Z",
              },
              thread_state: "answer_available",
              messages: [],
              answer_version: {
                id: "answer-1",
                chat_thread_id: "thread-1",
                retrieval_run_id: "retrieval-1",
                version_number: 1,
                answer_text: "Older approved-style draft",
                status: "draft",
                pipeline_profile_name: "default",
                pipeline_config_hash: "cfg-1",
                created_at: "2026-03-06T10:10:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v3",
                llm_capture_status: "captured",
                llm_request_text: "older render request",
                llm_response_text: "Older approved-style draft",
              },
              retrieval: null,
              evidence: [],
              failure_detail: null,
            }),
          );
        }

        if (
          url.pathname === "/api/cases/case-1/rows/row-1/raw-trace" &&
          method === "GET"
        ) {
          const scope = url.searchParams.get("scope");
          const answerVersionId = url.searchParams.get("answer_version_id");
          if (
            scope === "selected_answer_version" &&
            answerVersionId === "answer-1"
          ) {
            return new Response(
              JSON.stringify({
                scope: "selected_answer_version",
                row_id: "row-1",
                thread_id: "thread-1",
                execution_run_id: "run-1",
                answer_version_id: "answer-1",
                generation_path: "two_stage_plan_render",
                latest_attempt_state: "answer_available",
                failure_detail: null,
                planning_stage: {
                  availability: "available",
                  source_type: "current_run",
                  source_execution_run_id: "run-1",
                  source_answer_version_id: "answer-1",
                  model_invocation_id: "plan-1",
                  prompt_family: "answer_planning",
                  prompt_version: "answer_planning_prompt.v2",
                  requested_model_id: "gpt-5.2",
                  actual_model_id: "gpt-5.2",
                  reasoning_effort: "low",
                  temperature: null,
                  provider_response_id: "resp-plan-1",
                  service_tier: null,
                  usage_json: { input_tokens: 123, output_tokens: 45 },
                  request_payload_text: JSON.stringify([
                    {
                      role: "user",
                      content: [
                        {
                          type: "input_text",
                          text:
                            "<task>\nCreate a strict internal answer plan for one RfX questionnaire row.\nReturn only schema-valid JSON.\n</task>\n\n<normalized_evidence>\n[{\"id\":\"CF1\",\"title\":\"Scope item\"}]\n</normalized_evidence>",
                        },
                      ],
                    },
                  ]),
                  response_payload_text:
                    '{"plan":"selected","support_ids":["CF1","PT2"]}',
                },
                rendering_stage: {
                  availability: "available",
                  source_type: "current_run",
                  source_execution_run_id: "run-1",
                  source_answer_version_id: "answer-1",
                  model_invocation_id: "render-1",
                  prompt_family: "answer_rendering",
                  prompt_version: "answer_rendering_prompt.v3",
                  requested_model_id: "gpt-5.2",
                  actual_model_id: "gpt-5.2",
                  reasoning_effort: "low",
                  temperature: 0,
                  provider_response_id: "resp-render-1",
                  service_tier: null,
                  usage_json: { input_tokens: 210, output_tokens: 68 },
                  request_payload_text: "selected rendering request",
                  response_payload_text: "selected rendering response",
                },
              }),
            );
          }
          if (scope === "latest_attempt") {
            return new Response(
              JSON.stringify({
                scope: "latest_attempt",
                row_id: "row-1",
                thread_id: "thread-2",
                execution_run_id: "run-2",
                answer_version_id: "answer-2",
                generation_path: "two_stage_plan_render",
                latest_attempt_state: "answer_available",
                failure_detail: null,
                planning_stage: {
                  availability: "available",
                  source_type: "current_run",
                  source_execution_run_id: "run-2",
                  source_answer_version_id: "answer-2",
                  model_invocation_id: "plan-2",
                  prompt_family: "answer_planning",
                  prompt_version: "answer_planning_prompt.v2",
                  requested_model_id: "gpt-5.2",
                  actual_model_id: "gpt-5.2",
                  reasoning_effort: "medium",
                  temperature: null,
                  provider_response_id: "resp-plan-2",
                  service_tier: null,
                  usage_json: { input_tokens: 144, output_tokens: 55 },
                  request_payload_text: JSON.stringify([
                    {
                      role: "user",
                      content: [
                        {
                          type: "input_text",
                          text:
                            "<task>\nCreate the latest strict internal answer plan.\n</task>\n\n<normalized_evidence>\n[{\"id\":\"CF2\",\"title\":\"Latest scope item\"}]\n</normalized_evidence>",
                        },
                      ],
                    },
                  ]),
                  response_payload_text:
                    '{"plan":"latest","support_ids":["CF2","PT4"]}',
                },
                rendering_stage: {
                  availability: "available",
                  source_type: "current_run",
                  source_execution_run_id: "run-2",
                  source_answer_version_id: "answer-2",
                  model_invocation_id: "render-2",
                  prompt_family: "answer_rendering",
                  prompt_version: "answer_rendering_prompt.v3",
                  requested_model_id: "gpt-5.2",
                  actual_model_id: "gpt-5.2",
                  reasoning_effort: "medium",
                  temperature: 0,
                  provider_response_id: "resp-render-2",
                  service_tier: null,
                  usage_json: { input_tokens: 222, output_tokens: 74 },
                  request_payload_text: "latest rendering request",
                  response_payload_text: "latest rendering response",
                },
              }),
            );
          }
        }

        return new Response(
          JSON.stringify({ detail: `Unhandled request ${method} ${url}` }),
          { status: 500 },
        );
      },
    );

    render(<App />);

    await waitFor(() => {
      expect(screen.getByText("Questionnaire rows")).toBeInTheDocument();
    });
    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: "Revise wording" }),
      ).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: "Raw" }));

    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: "Selected version" }),
      ).toBeInTheDocument();
      expect(screen.getByText("Planning stage")).toBeInTheDocument();
      expect(
        screen.getByText(/Create a strict internal answer plan/i),
      ).toBeInTheDocument();
      expect(
        screen.getByText(/"id": "CF1"/),
      ).toBeInTheDocument();
      expect(
        screen.getByText(/<normalized_evidence>/),
      ).toBeInTheDocument();
      expect(
        screen.getByText(/selected rendering response/i),
      ).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: "Latest attempt" }));

    await waitFor(() => {
      expect(
        screen.getByText(/Create the latest strict internal answer plan/i),
      ).toBeInTheDocument();
      expect(
        screen.getByText(/"id": "CF2"/),
      ).toBeInTheDocument();
      expect(
        screen.getByText(/latest rendering response/i),
      ).toBeInTheDocument();
    });
  });

  it("lets reviewers expand the full row background in the selected-row hero", async () => {
    const longContext =
      "CrownShield Insurance Services Ltd is a UK and Ireland specialty insurance services provider with a distributed broker network. The requested scope covers broker onboarding, policy servicing requests, low-complexity claims triage, and secure exchange of policy and claims documents. The main objective is to reduce shared-mailbox volume and create a consistent operational audit trail.";

    fetchMock.mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        const method = init?.method ?? "GET";

        if (url.endsWith("/api/session/context")) {
          return new Response(
            JSON.stringify({
              tenant_id: "tenant-1",
              tenant_slug: "local-workspace",
              tenant_name: "Local Workspace",
              user_id: "user-1",
              user_email: "local.user.test",
              user_name: "Local Admin",
            }),
          );
        }

        if (url.endsWith("/api/cases") && method === "GET") {
          return new Response(
            JSON.stringify([
              {
                id: "case-1",
                name: "NordTransit Pilot",
                client_name: "NordTransit",
                language: "de",
                status: "active",
                created_at: "2026-03-06T10:00:00Z",
                updated_at: "2026-03-06T10:00:00Z",
              },
            ]),
          );
        }

        if (url.endsWith("/api/cases/case-1") && method === "GET") {
          return new Response(
            JSON.stringify({
              id: "case-1",
              name: "NordTransit Pilot",
              client_name: "NordTransit",
              language: "de",
              status: "active",
              created_at: "2026-03-06T10:00:00Z",
              updated_at: "2026-03-06T10:00:00Z",
              profile: null,
              latest_bulk_fill: null,
              questionnaire_rows: [
                {
                  id: "row-1",
                  source_row_id: "workbook:QA:2",
                  source_row_number: 2,
                  context: longContext,
                  question:
                    "Which products would you propose for the requested scope?",
                  current_answer: "",
                  review_status: "not_started",
                  approved_answer_version_id: null,
                  approved_answer_text: null,
                  last_error_detail: null,
                  last_bulk_fill_request_id: null,
                  last_bulk_fill_row_execution_id: null,
                  last_bulk_fill_status: null,
                  last_bulk_fill_attempt_number: null,
                  latest_attempt_thread_id: null,
                  latest_attempt_state: "none",
                },
              ],
              chats: [],
            }),
          );
        }

        if (
          url.endsWith("/api/cases/case-1/rows/row-1/answers") &&
          method === "GET"
        ) {
          return new Response(JSON.stringify([]));
        }

        return new Response(
          JSON.stringify({ detail: `Unhandled request ${method} ${url}` }),
          { status: 500 },
        );
      },
    );

    render(<App />);

    await waitFor(() => {
      expect(screen.getByText("Row background")).toBeInTheDocument();
    });

    expect(
      document.querySelector(".workspace-hero-row-label"),
    ).toHaveTextContent("Row 2");
    expect(
      document.querySelector(".workspace-hero-question"),
    ).toHaveTextContent(
      "Which products would you propose for the requested scope?",
    );
    expect(
      screen.getByRole("button", { name: "Show full background" }),
    ).toBeInTheDocument();
  });

  it("shows developer panels only when explicitly enabled", async () => {
    vi.stubEnv("VITE_ENABLE_DEV_PANELS", "true");

    fetchMock.mockImplementation(async (input: RequestInfo | URL) => {
      const url = String(input);

      if (url.endsWith("/api/session/context")) {
        return new Response(
          JSON.stringify({
            tenant_id: "tenant-1",
            tenant_slug: "local-workspace",
            tenant_name: "Local Workspace",
            user_id: "user-1",
            user_email: "local.user.test",
            user_name: "Local Admin",
          }),
        );
      }

      if (url.endsWith("/api/cases")) {
        return new Response(
          JSON.stringify([
            {
              id: "case-1",
              name: "NordTransit Pilot",
              client_name: "NordTransit",
              language: "de",
              status: "active",
              created_at: "2026-03-06T10:00:00Z",
              updated_at: "2026-03-06T10:00:00Z",
            },
          ]),
        );
      }

      if (url.endsWith("/api/cases/case-1")) {
        return new Response(
          JSON.stringify({
            id: "case-1",
            name: "NordTransit Pilot",
            client_name: "NordTransit",
            language: "de",
            status: "active",
            created_at: "2026-03-06T10:00:00Z",
            updated_at: "2026-03-06T10:00:00Z",
            profile: null,
            latest_bulk_fill: null,
            questionnaire_rows: [
              {
                id: "row-1",
                source_row_id: "workbook:QA:2",
                source_row_number: 2,
                context: "Shared context",
                question: "What fits the scope?",
                current_answer: "Draft answer body",
                review_status: "needs_review",
                approved_answer_version_id: null,
                approved_answer_text: null,
                last_error_detail: null,
              },
            ],
            chats: [
              {
                id: "thread-1",
                questionnaire_row_id: "row-1",
                title: "Row 2",
                updated_at: "2026-03-06T10:10:00Z",
              },
            ],
          }),
        );
      }

      if (url.endsWith("/api/cases/case-1/rows/row-1/answers")) {
        return new Response(
          JSON.stringify([
            {
              id: "answer-1",
              version_number: 1,
              answer_text: "Draft answer body",
              status: "draft",
              created_at: "2026-03-06T10:10:00Z",
              model: "stub-ai-service",
              generation_path: "two_stage_plan_render",
              llm_capture_stage: "answer_rendering",
              prompt_version: "answer_rendering_prompt.v2",
              llm_capture_status: "captured",
              llm_request_text: "Prompt body",
              llm_response_text: "Draft answer body",
            },
          ]),
        );
      }

      if (url.endsWith("/api/cases/case-1/threads/thread-1")) {
        return new Response(
          JSON.stringify({
            thread: {
              id: "thread-1",
              questionnaire_row_id: "row-1",
              title: "Row 2",
              updated_at: "2026-03-06T10:10:00Z",
            },
            messages: [],
            answer_version: {
              id: "answer-1",
              version_number: 1,
              answer_text: "Draft answer body",
              status: "draft",
              created_at: "2026-03-06T10:10:00Z",
              model: "stub-ai-service",
              generation_path: "two_stage_plan_render",
              llm_capture_stage: "answer_rendering",
              prompt_version: "answer_rendering_prompt.v2",
              llm_capture_status: "captured",
              llm_request_text: "Prompt body",
              llm_response_text: "Draft answer body",
            },
            retrieval: {
              strategy_version: "retrieval.v2.hardened.v1",
              revision_mode: "initial_draft",
              revision_classifier_version: "revision_classifier.v2",
              revision_reason: "no_previous_answer_version",
              retrieval_action: "refresh_retrieval",
              retrieval_action_reason: "new_or_content_change_requires_refresh",
              reused_from_retrieval_run_id: null,
              candidate_generation_mode: "sql_keyword_scope_python_rerank",
              broadened: false,
              sufficiency: "weak",
              degraded: false,
              notes: [],
              stages: [],
            },
            evidence: [],
          }),
        );
      }

      return new Response(JSON.stringify({ detail: "Unhandled request" }), {
        status: 500,
      });
    });

    render(<App />);

    await waitFor(() => {
      expect(screen.getByText("Developer tools")).toBeInTheDocument();
    });

    expect(screen.getByText("Database view")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Raw" })).toBeInTheDocument();
  });

  it("toggles the database view from the sidebar header button", async () => {
    fetchMock.mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        const method = init?.method ?? "GET";

        if (url.endsWith("/api/session/context")) {
          return new Response(
            JSON.stringify({
              tenant_id: "tenant-1",
              tenant_slug: "local-workspace",
              tenant_name: "Local Workspace",
              user_id: "user-1",
              user_email: "local.user.test",
              user_name: "Local Admin",
            }),
          );
        }

        if (url.endsWith("/api/cases") && method === "GET") {
          return new Response(
            JSON.stringify([
              {
                id: "case-1",
                name: "Crown",
                client_name: "CrownShield",
                language: "en",
                status: "active",
                created_at: "2026-03-06T10:00:00Z",
                updated_at: "2026-03-06T10:00:00Z",
              },
            ]),
          );
        }

        if (url.endsWith("/api/cases/case-1") && method === "GET") {
          return new Response(
            JSON.stringify({
              id: "case-1",
              name: "Crown",
              client_name: "CrownShield",
              language: "en",
              status: "active",
              created_at: "2026-03-06T10:00:00Z",
              updated_at: "2026-03-06T10:00:00Z",
              profile: null,
              latest_bulk_fill: null,
              bulk_fill_history: [],
              questionnaire_rows: [],
              chats: [],
            }),
          );
        }

        if (url.endsWith("/api/dev/tables") && method === "GET") {
          return new Response(
            JSON.stringify({
              tables: [
                {
                  name: "answer_versions",
                  row_count: 4,
                  case_filter_supported: true,
                },
                {
                  name: "execution_runs",
                  row_count: 12,
                  case_filter_supported: true,
                },
              ],
            }),
          );
        }

        if (
          url.includes("/api/dev/tables/answer_versions") &&
          method === "GET"
        ) {
          return new Response(
            JSON.stringify({
              table_name: "answer_versions",
              row_count: 4,
              case_filter_applied: true,
              columns: ["id", "status", "version_number"],
              rows: [
                {
                  id: "answer-1",
                  status: "accepted",
                  version_number: 1,
                },
              ],
            }),
          );
        }

        return new Response(JSON.stringify({ detail: "Unhandled request" }), {
          status: 500,
        });
      },
    );

    render(<App />);

    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: "Database" }),
      ).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: "Database" }));

    await waitFor(() => {
      expect(
        screen.getByRole("heading", { name: "Backend records" }),
      ).toBeInTheDocument();
      expect(
        screen.getByRole("heading", { name: "Answer Versions" }),
      ).toBeInTheDocument();
      expect(screen.getByText("accepted")).toBeInTheDocument();
    });

    expect(
      screen.getByText(
        "Inspect scoped workflow state, lineage artifacts, and export surfaces without leaving the drafting workstation.",
      ),
    ).toBeInTheDocument();
    expect(screen.getAllByText("answer_versions")[0]).toBeInTheDocument();
    expect(
      screen.getByText(
        "Reviewing case-scoped rows for Crown where table lineage supports case filters.",
      ),
    ).toBeInTheDocument();
    expect(screen.getByText("preview 50 rows")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Database" }));

    await waitFor(() => {
      expect(
        screen.queryByRole("heading", { name: "Backend records" }),
      ).not.toBeInTheDocument();
      expect(
        screen.getByRole("heading", { name: "Questionnaire rows" }),
      ).toBeInTheDocument();
    });
  });

  it("disables drafting controls while waiting for the model response", async () => {
    let resolveDraftResponse!: (value: Response) => void;
    let draftResolved = false;
    fetchMock.mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        const method = init?.method ?? "GET";

        if (url.endsWith("/api/session/context")) {
          return new Response(
            JSON.stringify({
              tenant_id: "tenant-1",
              tenant_slug: "local-workspace",
              tenant_name: "Local Workspace",
              user_id: "user-1",
              user_email: "local.user.test",
              user_name: "Local Admin",
            }),
          );
        }

        if (url.endsWith("/api/cases") && method === "GET") {
          return new Response(
            JSON.stringify([
              {
                id: "case-1",
                name: "NordTransit Pilot",
                client_name: "NordTransit",
                language: "de",
                status: "active",
                created_at: "2026-03-06T10:00:00Z",
                updated_at: "2026-03-06T10:00:00Z",
              },
            ]),
          );
        }

        if (url.endsWith("/api/cases/case-1") && method === "GET") {
          return new Response(
            JSON.stringify({
              id: "case-1",
              name: "NordTransit Pilot",
              client_name: "NordTransit",
              language: "de",
              status: "active",
              created_at: "2026-03-06T10:00:00Z",
              updated_at: "2026-03-06T10:00:00Z",
              profile: null,
              latest_bulk_fill: null,
              questionnaire_rows: [
                {
                  id: "row-1",
                  source_row_id: "workbook:QA:2",
                  source_row_number: 2,
                  context: "Shared context",
                  question: "What fits the scope?",
                  current_answer: draftResolved ? "Draft answer body" : "",
                  review_status: draftResolved ? "needs_review" : "not_started",
                  approved_answer_version_id: null,
                  approved_answer_text: null,
                  last_error_detail: null,
                },
              ],
              chats: [],
            }),
          );
        }

        if (
          url.endsWith("/api/cases/case-1/rows/row-1/answers") &&
          method === "GET"
        ) {
          return new Response(
            JSON.stringify(
              draftResolved
                ? [
                    {
                      id: "answer-1",
                      version_number: 1,
                      answer_text: "Draft answer body",
                      status: "draft",
                      created_at: "2026-03-06T10:10:00Z",
                      model: "stub-ai-service",
                      generation_path: "two_stage_plan_render",
                      llm_capture_stage: "answer_rendering",
                      prompt_version: "answer_rendering_prompt.v2",
                      llm_capture_status: "captured",
                      llm_request_text: "Prompt body",
                      llm_response_text: "Draft answer body",
                    },
                  ]
                : [],
            ),
          );
        }

        if (
          url.endsWith("/api/cases/case-1/rows/row-1/draft") &&
          method === "POST"
        ) {
          return new Promise<Response>((resolve) => {
            resolveDraftResponse = resolve;
          });
        }

        return new Response(
          JSON.stringify({ detail: `Unhandled request ${method} ${url}` }),
          {
            status: 500,
          },
        );
      },
    );

    render(<App />);

    await waitFor(() => {
      expect(screen.getByText("Questionnaire rows")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText("Generate answer"));

    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: "Waiting for model response..." }),
      ).toBeDisabled();
    });

    draftResolved = true;
    resolveDraftResponse(
      new Response(
        JSON.stringify({
          thread: {
            id: "thread-1",
            questionnaire_row_id: "row-1",
            title: "Row 2",
            updated_at: "2026-03-06T10:10:00Z",
          },
          messages: [],
          answer_version: {
            id: "answer-1",
            version_number: 1,
            answer_text: "Draft answer body",
            status: "draft",
            created_at: "2026-03-06T10:10:00Z",
            model: "stub-ai-service",
            generation_path: "two_stage_plan_render",
            llm_capture_stage: "answer_rendering",
            prompt_version: "answer_rendering_prompt.v2",
            llm_capture_status: "captured",
            llm_request_text: "Prompt body",
            llm_response_text: "Draft answer body",
          },
          retrieval: {
            strategy_version: "retrieval.v2.hardened.v1",
            revision_mode: "initial_draft",
            revision_classifier_version: "revision_classifier.v2",
            revision_reason: "no_previous_answer_version",
            retrieval_action: "refresh_retrieval",
            retrieval_action_reason: "new_or_content_change_requires_refresh",
            reused_from_retrieval_run_id: null,
            candidate_generation_mode: "sql_keyword_scope_python_rerank",
            broadened: false,
            sufficiency: "weak",
            degraded: false,
            notes: [],
            stages: [],
          },
          evidence: [],
        }),
      ),
    );

    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: "Revise wording" }),
      ).toBeEnabled();
      expect(
        screen.getByRole("button", { name: "Regenerate content" }),
      ).toBeEnabled();
      expect(
        screen.queryByPlaceholderText(
          "Ask for a style revision such as shorter, clearer, or more formal.",
        ),
      ).not.toBeInTheDocument();
    });
  });

  it("accepts render-only revise responses without changing the button contract", async () => {
    let revised = false;
    let revisePayload: string | null = null;
    fetchMock.mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        const method = init?.method ?? "GET";

        if (url.endsWith("/api/session/context")) {
          return new Response(
            JSON.stringify({
              tenant_id: "tenant-1",
              tenant_slug: "local-workspace",
              tenant_name: "Local Workspace",
              user_id: "user-1",
              user_email: "local.user.test",
              user_name: "Local Admin",
            }),
          );
        }

        if (url.endsWith("/api/cases") && method === "GET") {
          return new Response(
            JSON.stringify([
              {
                id: "case-1",
                name: "NordTransit Pilot",
                client_name: "NordTransit",
                language: "de",
                status: "active",
                created_at: "2026-03-06T10:00:00Z",
                updated_at: "2026-03-06T10:00:00Z",
              },
            ]),
          );
        }

        if (url.endsWith("/api/cases/case-1") && method === "GET") {
          return new Response(
            JSON.stringify({
              id: "case-1",
              name: "NordTransit Pilot",
              client_name: "NordTransit",
              language: "de",
              status: "active",
              created_at: "2026-03-06T10:00:00Z",
              updated_at: "2026-03-06T10:00:00Z",
              profile: {
                schema_version: "rfx_case_profile.v3",
                prompt_set_version: "rfx_case_profile_prompt_set.v3",
                summary: "Pilot summary",
                generated_at: "2026-03-06T10:00:00Z",
              },
              latest_bulk_fill: null,
              questionnaire_rows: [
                {
                  id: "row-1",
                  source_row_id: "workbook:QA:2",
                  source_row_number: 2,
                  context: "Shared context",
                  question: "What fits the scope?",
                  current_answer: revised
                    ? "Flowing revised answer"
                    : "Draft answer body",
                  review_status: "needs_review",
                  approved_answer_version_id: null,
                  approved_answer_text: null,
                  last_error_detail: null,
                },
              ],
              chats: [
                {
                  id: "thread-1",
                  questionnaire_row_id: "row-1",
                  title: "Row 2",
                  updated_at: "2026-03-06T10:10:00Z",
                },
              ],
            }),
          );
        }

        if (
          url.endsWith("/api/cases/case-1/rows/row-1/answers") &&
          method === "GET"
        ) {
          return new Response(
            JSON.stringify(
              revised
                ? [
                    {
                      id: "answer-2",
                      version_number: 2,
                      answer_text: "Flowing revised answer",
                      status: "draft",
                      created_at: "2026-03-06T10:12:00Z",
                      model: "stub-ai-service",
                      generation_path: "render_only_reuse_plan",
                      llm_capture_stage: "answer_rendering",
                      prompt_version: "answer_rendering_prompt.v2",
                      llm_capture_status: "captured",
                      llm_request_text: "Prompt body",
                      llm_response_text: "Flowing revised answer",
                    },
                    {
                      id: "answer-1",
                      version_number: 1,
                      answer_text: "Draft answer body",
                      status: "draft",
                      created_at: "2026-03-06T10:10:00Z",
                      model: "stub-ai-service",
                      generation_path: "two_stage_plan_render",
                      llm_capture_stage: "answer_rendering",
                      prompt_version: "answer_rendering_prompt.v2",
                      llm_capture_status: "captured",
                      llm_request_text: "Prompt body",
                      llm_response_text: "Draft answer body",
                    },
                  ]
                : [
                    {
                      id: "answer-1",
                      version_number: 1,
                      answer_text: "Draft answer body",
                      status: "draft",
                      created_at: "2026-03-06T10:10:00Z",
                      model: "stub-ai-service",
                      generation_path: "two_stage_plan_render",
                      llm_capture_stage: "answer_rendering",
                      prompt_version: "answer_rendering_prompt.v2",
                      llm_capture_status: "captured",
                      llm_request_text: "Prompt body",
                      llm_response_text: "Draft answer body",
                    },
                  ],
            ),
          );
        }

        if (
          url.endsWith("/api/cases/case-1/threads/thread-1") &&
          method === "GET"
        ) {
          return new Response(
            JSON.stringify({
              thread: {
                id: "thread-1",
                questionnaire_row_id: "row-1",
                title: "Row 2",
                updated_at: "2026-03-06T10:12:00Z",
              },
              messages: revised
                ? [
                    {
                      id: "message-1",
                      role: "user",
                      content: "Generate a grounded answer for this row.",
                      created_at: "2026-03-06T10:10:00Z",
                    },
                    {
                      id: "message-2",
                      role: "assistant",
                      content: "Draft answer body",
                      created_at: "2026-03-06T10:10:10Z",
                    },
                    {
                      id: "message-3",
                      role: "user",
                      content:
                        "ok but remove the bullet points. just a flowing text",
                      created_at: "2026-03-06T10:12:00Z",
                    },
                    {
                      id: "message-4",
                      role: "assistant",
                      content: "Flowing revised answer",
                      created_at: "2026-03-06T10:12:05Z",
                    },
                  ]
                : [
                    {
                      id: "message-1",
                      role: "user",
                      content: "Generate a grounded answer for this row.",
                      created_at: "2026-03-06T10:10:00Z",
                    },
                    {
                      id: "message-2",
                      role: "assistant",
                      content: "Draft answer body",
                      created_at: "2026-03-06T10:10:10Z",
                    },
                  ],
              answer_version: revised
                ? {
                    id: "answer-2",
                    version_number: 2,
                    answer_text: "Flowing revised answer",
                    status: "draft",
                    created_at: "2026-03-06T10:12:00Z",
                    model: "stub-ai-service",
                    generation_path: "render_only_reuse_plan",
                    llm_capture_stage: "answer_rendering",
                    prompt_version: "answer_rendering_prompt.v2",
                    llm_capture_status: "captured",
                    llm_request_text: "Prompt body",
                    llm_response_text: "Flowing revised answer",
                  }
                : {
                    id: "answer-1",
                    version_number: 1,
                    answer_text: "Draft answer body",
                    status: "draft",
                    created_at: "2026-03-06T10:10:00Z",
                    model: "stub-ai-service",
                    generation_path: "two_stage_plan_render",
                    llm_capture_stage: "answer_rendering",
                    prompt_version: "answer_rendering_prompt.v2",
                    llm_capture_status: "captured",
                    llm_request_text: "Prompt body",
                    llm_response_text: "Draft answer body",
                  },
              retrieval: {
                strategy_version: "retrieval.v2.hardened.v1",
                revision_mode: "initial_draft",
                revision_classifier_version: "revision_classifier.v2",
                revision_reason: "no_previous_answer_version",
                retrieval_action: "refresh_retrieval",
                retrieval_action_reason:
                  "new_or_content_change_requires_refresh",
                reused_from_retrieval_run_id: null,
                candidate_generation_mode: "sql_keyword_scope_python_rerank",
                broadened: false,
                sufficiency: "weak",
                degraded: false,
                notes: [],
                stages: [],
              },
              evidence: [],
            }),
          );
        }

        if (
          url.endsWith("/api/cases/case-1/rows/row-1/draft") &&
          method === "POST"
        ) {
          revisePayload = String(init?.body ?? "");
          revised = true;
          return new Response(
            JSON.stringify({
              thread: {
                id: "thread-1",
                questionnaire_row_id: "row-1",
                title: "Row 2",
                updated_at: "2026-03-06T10:12:00Z",
              },
              messages: [
                {
                  id: "message-1",
                  role: "user",
                  content: "Generate a grounded answer for this row.",
                  created_at: "2026-03-06T10:10:00Z",
                },
                {
                  id: "message-2",
                  role: "assistant",
                  content: "Draft answer body",
                  created_at: "2026-03-06T10:10:10Z",
                },
                {
                  id: "message-3",
                  role: "user",
                  content:
                    "ok but remove the bullet points. just a flowing text",
                  created_at: "2026-03-06T10:12:00Z",
                },
                {
                  id: "message-4",
                  role: "assistant",
                  content: "Flowing revised answer",
                  created_at: "2026-03-06T10:12:05Z",
                },
              ],
              answer_version: {
                id: "answer-2",
                version_number: 2,
                answer_text: "Flowing revised answer",
                status: "draft",
                created_at: "2026-03-06T10:12:00Z",
                model: "stub-ai-service",
                generation_path: "render_only_reuse_plan",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v2",
                llm_capture_status: "captured",
                llm_request_text: "Prompt body",
                llm_response_text: "Flowing revised answer",
              },
              retrieval: {
                strategy_version: "retrieval.v2.hardened.v1",
                revision_mode: "initial_draft",
                revision_classifier_version: "revision_classifier.v2",
                revision_reason: "no_previous_answer_version",
                retrieval_action: "refresh_retrieval",
                retrieval_action_reason:
                  "new_or_content_change_requires_refresh",
                reused_from_retrieval_run_id: null,
                candidate_generation_mode: "sql_keyword_scope_python_rerank",
                broadened: false,
                sufficiency: "weak",
                degraded: false,
                notes: [],
                stages: [],
              },
              evidence: [],
            }),
          );
        }

        return new Response(
          JSON.stringify({ detail: `Unhandled request ${method} ${url}` }),
          { status: 500 },
        );
      },
    );

    render(<App />);

    await waitFor(() => {
      expect(screen.getByText("Revise wording")).toBeInTheDocument();
    });
    const chatLog = screen.getByLabelText("Conversation history");
    Object.defineProperty(chatLog, "scrollHeight", {
      configurable: true,
      get: () => 320,
    });
    chatLog.scrollTop = 0;

    fireEvent.click(screen.getByRole("button", { name: "Revise wording" }));
    const revisionInput = await screen.findByPlaceholderText(
      "Ask for a style revision such as shorter, clearer, or more formal.",
    );
    fireEvent.change(revisionInput, {
      target: {
        value: "ok but remove the bullet points. just a flowing text",
      },
    });
    fireEvent.click(screen.getByText("Revise answer"));

    await waitFor(() => {
      expect(
        screen.getAllByText("Flowing revised answer")[0],
      ).toBeInTheDocument();
      expect(chatLog.scrollTop).toBe(320);
    });

    expect(JSON.parse(revisePayload ?? "")).toEqual({
      message: "ok but remove the bullet points. just a flowing text",
      thread_id: "thread-1",
      revision_mode_override: "style_only",
    });
  });

  it("renders bulk-fill summary and row review status", async () => {
    fetchMock.mockImplementation(async (input: RequestInfo | URL) => {
      const url = String(input);

      if (url.endsWith("/api/session/context")) {
        return new Response(
          JSON.stringify({
            tenant_id: "tenant-1",
            tenant_slug: "local-workspace",
            tenant_name: "Local Workspace",
            user_id: "user-1",
            user_email: "local.user.test",
            user_name: "Local Admin",
          }),
        );
      }

      if (url.endsWith("/api/cases")) {
        return new Response(
          JSON.stringify([
            {
              id: "case-1",
              name: "NordTransit Pilot",
              client_name: "NordTransit",
              language: "de",
              status: "active",
              created_at: "2026-03-06T10:00:00Z",
              updated_at: "2026-03-06T10:00:00Z",
            },
          ]),
        );
      }

      if (url.endsWith("/api/cases/case-1")) {
        return new Response(
          JSON.stringify({
            id: "case-1",
            name: "NordTransit Pilot",
            client_name: "NordTransit",
            language: "de",
            status: "active",
            created_at: "2026-03-06T10:00:00Z",
            updated_at: "2026-03-06T10:00:00Z",
            profile: null,
            latest_bulk_fill: {
              id: "bulk-1",
              parent_request_id: null,
              status: "running",
              created_at: "2026-03-06T10:00:00Z",
              updated_at: "2026-03-06T10:01:00Z",
              summary: {
                total_rows: 1,
                row_execution_counts: {
                  not_started: 0,
                  running: 1,
                  drafted: 0,
                  failed: 0,
                  skipped: 0,
                  cancelled: 0,
                },
                review_status_counts: {
                  not_started: 0,
                  running: 1,
                  needs_review: 0,
                  approved: 0,
                  rejected: 0,
                  failed: 0,
                  skipped: 0,
                },
              },
              error_detail: null,
              config: {},
            },
            questionnaire_rows: [
              {
                id: "row-1",
                source_row_id: "workbook:QA:2",
                source_row_number: 2,
                context: "Shared context",
                question: "What fits the scope?",
                current_answer: "Draft answer body",
                review_status: "needs_review",
                approved_answer_version_id: null,
                approved_answer_text: null,
                last_error_detail: null,
              },
            ],
            chats: [],
          }),
        );
      }

      if (url.endsWith("/api/cases/case-1/rows/row-1/answers")) {
        return new Response(
          JSON.stringify([
            {
              id: "answer-1",
              version_number: 1,
              answer_text: "Draft answer body",
              status: "draft",
              created_at: "2026-03-06T10:10:00Z",
              model: "stub-ai-service",
              generation_path: "two_stage_plan_render",
              llm_capture_stage: "answer_rendering",
              prompt_version: "answer_rendering_prompt.v2",
              llm_capture_status: "captured",
              llm_request_text: "Prompt body",
              llm_response_text: "Draft answer body",
            },
          ]),
        );
      }

      return new Response(JSON.stringify({ detail: "Unhandled request" }), {
        status: 500,
      });
    });

    render(<App />);

    await waitFor(() => {
      expect(screen.getByText("Latest bulk-fill")).toBeInTheDocument();
      expect(screen.getAllByText("running")[0]).toBeInTheDocument();
      expect(screen.getAllByText("needs review")[0]).toBeInTheDocument();
    });
  });

  it("warns before bulk-fill when unapproved work would be overwritten", async () => {
    const confirmMock = vi.fn(() => false);
    vi.stubGlobal("confirm", confirmMock);
    let bulkFillPosted = false;
    fetchMock.mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        const method = init?.method ?? "GET";

        if (url.endsWith("/api/session/context")) {
          return new Response(
            JSON.stringify({
              tenant_id: "tenant-1",
              tenant_slug: "local-workspace",
              tenant_name: "Local Workspace",
              user_id: "user-1",
              user_email: "local.user.test",
              user_name: "Local Admin",
            }),
          );
        }

        if (url.endsWith("/api/cases") && method === "GET") {
          return new Response(
            JSON.stringify([
              {
                id: "case-1",
                name: "NordTransit Pilot",
                client_name: "NordTransit",
                language: "de",
                status: "active",
                created_at: "2026-03-06T10:00:00Z",
                updated_at: "2026-03-06T10:00:00Z",
              },
            ]),
          );
        }

        if (url.endsWith("/api/cases/case-1") && method === "GET") {
          return new Response(
            JSON.stringify({
              id: "case-1",
              name: "NordTransit Pilot",
              client_name: "NordTransit",
              language: "de",
              status: "active",
              created_at: "2026-03-06T10:00:00Z",
              updated_at: "2026-03-06T10:00:00Z",
              profile: null,
              latest_bulk_fill: null,
              bulk_fill_history: [],
              questionnaire_rows: [
                {
                  id: "row-1",
                  source_row_id: "workbook:QA:2",
                  source_row_number: 2,
                  context: "Shared context",
                  question: "What fits the scope?",
                  current_answer: "Approved answer",
                  review_status: "approved",
                  approved_answer_version_id: "approved-answer-1",
                  approved_answer_text: "Approved answer",
                  last_error_detail: null,
                },
                {
                  id: "row-2",
                  source_row_id: "workbook:QA:3",
                  source_row_number: 3,
                  context: "Shared context",
                  question: "What else fits the scope?",
                  current_answer: "Existing unapproved draft",
                  review_status: "needs_review",
                  approved_answer_version_id: null,
                  approved_answer_text: null,
                  last_error_detail: null,
                },
                {
                  id: "row-3",
                  source_row_id: "workbook:QA:4",
                  source_row_number: 4,
                  context: "Shared context",
                  question: "How would you roll out?",
                  current_answer: "",
                  review_status: "not_started",
                  approved_answer_version_id: null,
                  approved_answer_text: null,
                  last_error_detail: null,
                },
              ],
              chats: [
                {
                  id: "thread-2",
                  questionnaire_row_id: "row-2",
                  title: "Row 3",
                  updated_at: "2026-03-06T10:10:00Z",
                },
              ],
            }),
          );
        }

        if (url.endsWith("/api/cases/case-1/rows/row-1/answers")) {
          return new Response(JSON.stringify([]));
        }

        if (url.endsWith("/api/cases/case-1/bulk-fill") && method === "POST") {
          bulkFillPosted = true;
          return new Response(JSON.stringify({ detail: "Unexpected POST" }), {
            status: 500,
          });
        }

        return new Response(JSON.stringify({ detail: "Unhandled request" }), {
          status: 500,
        });
      },
    );

    render(<App />);

    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: "Launch bulk-fill" }),
      ).toBeEnabled();
    });

    fireEvent.click(screen.getByRole("button", { name: "Launch bulk-fill" }));

    await waitFor(() => {
      expect(confirmMock).toHaveBeenCalledTimes(1);
      expect(
        screen.getByText("Bulk-fill launch cancelled."),
      ).toBeInTheDocument();
    });
    expect(bulkFillPosted).toBe(false);
  });

  it("launches bulk-fill without warning for untouched rows and refreshes the status line", async () => {
    const confirmMock = vi.fn(() => true);
    vi.stubGlobal("confirm", confirmMock);
    let caseDetailLoads = 0;
    fetchMock.mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        const method = init?.method ?? "GET";

        if (url.endsWith("/api/session/context")) {
          return new Response(
            JSON.stringify({
              tenant_id: "tenant-1",
              tenant_slug: "local-workspace",
              tenant_name: "Local Workspace",
              user_id: "user-1",
              user_email: "local.user.test",
              user_name: "Local Admin",
            }),
          );
        }

        if (url.endsWith("/api/cases") && method === "GET") {
          return new Response(
            JSON.stringify([
              {
                id: "case-1",
                name: "NordTransit Pilot",
                client_name: "NordTransit",
                language: "de",
                status: "active",
                created_at: "2026-03-06T10:00:00Z",
                updated_at: "2026-03-06T10:00:00Z",
              },
            ]),
          );
        }

        if (url.endsWith("/api/cases/case-1") && method === "GET") {
          caseDetailLoads += 1;
          return new Response(
            JSON.stringify({
              id: "case-1",
              name: "NordTransit Pilot",
              client_name: "NordTransit",
              language: "de",
              status: "active",
              created_at: "2026-03-06T10:00:00Z",
              updated_at: "2026-03-06T10:00:00Z",
              profile: null,
              latest_bulk_fill:
                caseDetailLoads >= 2
                  ? {
                      id: "bulk-1",
                      parent_request_id: null,
                      status: "completed_with_failures",
                      created_at: "2026-03-06T10:00:00Z",
                      updated_at: "2026-03-06T10:01:00Z",
                      claim_id: null,
                      runner_id: "bulk-fill-worker.local",
                      execution_mode: "worker_cli",
                      claimed_at: null,
                      started_at: null,
                      heartbeat_at: null,
                      finished_at: "2026-03-06T10:01:00Z",
                      cancel_requested_at: null,
                      stale_detected_at: null,
                      summary: {
                        total_rows: 2,
                        row_execution_counts: {
                          not_started: 0,
                          running: 0,
                          drafted: 0,
                          failed: 2,
                          skipped: 0,
                          cancelled: 0,
                        },
                        review_status_counts: {
                          not_started: 0,
                          running: 0,
                          needs_review: 0,
                          approved: 0,
                          rejected: 0,
                          failed: 2,
                          skipped: 0,
                        },
                      },
                      error_detail: null,
                      config: {},
                    }
                  : null,
              bulk_fill_history: [],
              questionnaire_rows: [
                {
                  id: "row-1",
                  source_row_id: "workbook:QA:2",
                  source_row_number: 2,
                  context: "Shared context",
                  question: "What fits the scope?",
                  current_answer: "",
                  review_status: "not_started",
                  approved_answer_version_id: null,
                  approved_answer_text: null,
                  last_error_detail: null,
                },
                {
                  id: "row-2",
                  source_row_id: "workbook:QA:3",
                  source_row_number: 3,
                  context: "Shared context",
                  question: "What else fits the scope?",
                  current_answer: "",
                  review_status: "not_started",
                  approved_answer_version_id: null,
                  approved_answer_text: null,
                  last_error_detail: null,
                },
              ],
              chats: [],
            }),
          );
        }

        if (url.endsWith("/api/cases/case-1/rows/row-1/answers")) {
          return new Response(JSON.stringify([]));
        }

        if (url.endsWith("/api/cases/case-1/bulk-fill") && method === "POST") {
          return new Response(
            JSON.stringify({
              request: {
                id: "bulk-1",
                parent_request_id: null,
                status: "queued",
                created_at: "2026-03-06T10:00:00Z",
                updated_at: "2026-03-06T10:00:00Z",
                claim_id: null,
                runner_id: null,
                execution_mode: null,
                claimed_at: null,
                started_at: null,
                heartbeat_at: null,
                finished_at: null,
                cancel_requested_at: null,
                stale_detected_at: null,
                summary: {},
                error_detail: null,
                config: {},
              },
            }),
          );
        }

        return new Response(JSON.stringify({ detail: "Unhandled request" }), {
          status: 500,
        });
      },
    );

    render(<App />);

    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: "Launch bulk-fill" }),
      ).toBeEnabled();
    });

    fireEvent.click(screen.getByRole("button", { name: "Launch bulk-fill" }));

    await waitFor(() => {
      expect(confirmMock).not.toHaveBeenCalled();
      expect(
        screen.getByText("Bulk-fill request completed_with_failures."),
      ).toBeInTheDocument();
    });
  });

  it("shows placeholder counts after approved export", async () => {
    const createObjectURLMock = vi.fn(() => "blob:approved-export");
    const revokeObjectURLMock = vi.fn();
    const clickMock = vi
      .spyOn(HTMLAnchorElement.prototype, "click")
      .mockImplementation(() => {});
    vi.stubGlobal("URL", {
      createObjectURL: createObjectURLMock,
      revokeObjectURL: revokeObjectURLMock,
    });

    fetchMock.mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        const method = init?.method ?? "GET";

        if (url.endsWith("/api/session/context")) {
          return new Response(
            JSON.stringify({
              tenant_id: "tenant-1",
              tenant_slug: "local-workspace",
              tenant_name: "Local Workspace",
              user_id: "user-1",
              user_email: "local.user.test",
              user_name: "Local Admin",
            }),
          );
        }

        if (url.endsWith("/api/cases") && method === "GET") {
          return new Response(
            JSON.stringify([
              {
                id: "case-1",
                name: "NordTransit Pilot",
                client_name: "NordTransit",
                language: "de",
                status: "active",
                created_at: "2026-03-06T10:00:00Z",
                updated_at: "2026-03-06T10:00:00Z",
              },
            ]),
          );
        }

        if (url.endsWith("/api/cases/case-1") && method === "GET") {
          return new Response(
            JSON.stringify({
              id: "case-1",
              name: "NordTransit Pilot",
              client_name: "NordTransit",
              language: "de",
              status: "active",
              created_at: "2026-03-06T10:00:00Z",
              updated_at: "2026-03-06T10:00:00Z",
              profile: {
                schema_version: "rfx_case_profile.v3",
                prompt_set_version: "rfx_case_profile_prompt_set.v3",
                summary: "Pilot summary",
                generated_at: "2026-03-06T10:00:00Z",
              },
              latest_bulk_fill: null,
              questionnaire_rows: [],
              chats: [],
            }),
          );
        }

        if (url.endsWith("/api/cases/case-1/export") && method === "POST") {
          return new Response(
            JSON.stringify({
              export_job_id: "export-1",
              status: "completed",
              export_mode: "approved_only",
              includes_unapproved_drafts: false,
              placeholder_row_count: 3,
              download_upload_id: "upload-1",
              csv_download_upload_id: "upload-1-csv",
              zip_download_upload_id: "upload-1-zip",
            }),
          );
        }

        if (
          url.endsWith("/api/cases/downloads/upload-1-zip") &&
          method === "GET"
        ) {
          return new Response(new Uint8Array([1, 2, 3]), {
            headers: {
              "content-disposition":
                'attachment; filename="approved-export.zip"',
            },
          });
        }

        return new Response(JSON.stringify({ detail: "Unhandled request" }), {
          status: 500,
        });
      },
    );

    render(<App />);

    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: "Export approved" }),
      ).toBeEnabled();
    });

    fireEvent.click(screen.getByRole("button", { name: "Export approved" }));

    await waitFor(() => {
      expect(
        screen.getByText(
          "Export downloaded with mode approved_only (3 placeholder rows).",
        ),
      ).toBeInTheDocument();
    });
    expect(createObjectURLMock).toHaveBeenCalledTimes(1);
    expect(revokeObjectURLMock).toHaveBeenCalledWith("blob:approved-export");
    clickMock.mockRestore();
  });

  it("keeps draft messaging for latest export", async () => {
    const createObjectURLMock = vi.fn(() => "blob:latest-export");
    const revokeObjectURLMock = vi.fn();
    const clickMock = vi
      .spyOn(HTMLAnchorElement.prototype, "click")
      .mockImplementation(() => {});
    vi.stubGlobal("URL", {
      createObjectURL: createObjectURLMock,
      revokeObjectURL: revokeObjectURLMock,
    });

    fetchMock.mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        const method = init?.method ?? "GET";

        if (url.endsWith("/api/session/context")) {
          return new Response(
            JSON.stringify({
              tenant_id: "tenant-1",
              tenant_slug: "local-workspace",
              tenant_name: "Local Workspace",
              user_id: "user-1",
              user_email: "local.user.test",
              user_name: "Local Admin",
            }),
          );
        }

        if (url.endsWith("/api/cases") && method === "GET") {
          return new Response(
            JSON.stringify([
              {
                id: "case-1",
                name: "NordTransit Pilot",
                client_name: "NordTransit",
                language: "de",
                status: "active",
                created_at: "2026-03-06T10:00:00Z",
                updated_at: "2026-03-06T10:00:00Z",
              },
            ]),
          );
        }

        if (url.endsWith("/api/cases/case-1") && method === "GET") {
          return new Response(
            JSON.stringify({
              id: "case-1",
              name: "NordTransit Pilot",
              client_name: "NordTransit",
              language: "de",
              status: "active",
              created_at: "2026-03-06T10:00:00Z",
              updated_at: "2026-03-06T10:00:00Z",
              profile: {
                schema_version: "rfx_case_profile.v3",
                prompt_set_version: "rfx_case_profile_prompt_set.v3",
                summary: "Pilot summary",
                generated_at: "2026-03-06T10:00:00Z",
              },
              latest_bulk_fill: null,
              questionnaire_rows: [],
              chats: [],
            }),
          );
        }

        if (url.endsWith("/api/cases/case-1/export") && method === "POST") {
          return new Response(
            JSON.stringify({
              export_job_id: "export-2",
              status: "completed",
              export_mode: "latest_available",
              includes_unapproved_drafts: true,
              placeholder_row_count: 4,
              download_upload_id: "upload-2",
              csv_download_upload_id: "upload-2-csv",
              zip_download_upload_id: "upload-2-zip",
            }),
          );
        }

        if (
          url.endsWith("/api/cases/downloads/upload-2-zip") &&
          method === "GET"
        ) {
          return new Response(new Uint8Array([4, 5, 6]), {
            headers: {
              "content-disposition": 'attachment; filename="latest-export.zip"',
            },
          });
        }

        return new Response(JSON.stringify({ detail: "Unhandled request" }), {
          status: 500,
        });
      },
    );

    render(<App />);

    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: "Export latest" }),
      ).toBeEnabled();
    });

    fireEvent.click(screen.getByRole("button", { name: "Export latest" }));

    await waitFor(() => {
      expect(
        screen.getByText(
          "Export downloaded with mode latest_available (includes drafts, 4 placeholder rows).",
        ),
      ).toBeInTheDocument();
    });
    expect(createObjectURLMock).toHaveBeenCalledTimes(1);
    expect(revokeObjectURLMock).toHaveBeenCalledWith("blob:latest-export");
    clickMock.mockRestore();
  });

  it("clears stale answer state when switching to a failed no-answer row", async () => {
    fetchMock.mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        const method = init?.method ?? "GET";

        if (url.endsWith("/api/session/context")) {
          return new Response(
            JSON.stringify({
              tenant_id: "tenant-1",
              tenant_slug: "local-workspace",
              tenant_name: "Local Workspace",
              user_id: "user-1",
              user_email: "local.user.test",
              user_name: "Local Admin",
            }),
          );
        }

        if (url.endsWith("/api/cases") && method === "GET") {
          return new Response(
            JSON.stringify([
              {
                id: "case-1",
                name: "NordTransit Pilot",
                client_name: "NordTransit",
                language: "de",
                status: "active",
                created_at: "2026-03-06T10:00:00Z",
                updated_at: "2026-03-06T10:00:00Z",
              },
            ]),
          );
        }

        if (url.endsWith("/api/cases/case-1") && method === "GET") {
          return new Response(
            JSON.stringify({
              id: "case-1",
              name: "NordTransit Pilot",
              client_name: "NordTransit",
              language: "de",
              status: "active",
              created_at: "2026-03-06T10:00:00Z",
              updated_at: "2026-03-06T10:00:00Z",
              profile: null,
              latest_bulk_fill: null,
              bulk_fill_history: [],
              questionnaire_rows: [
                {
                  id: "row-1",
                  source_row_id: "workbook:QA:2",
                  source_row_number: 2,
                  context: "Shared context",
                  question: "What fits the scope?",
                  current_answer: "Row one draft body",
                  review_status: "needs_review",
                  approved_answer_version_id: null,
                  approved_answer_text: null,
                  last_error_detail: null,
                  last_bulk_fill_request_id: null,
                  last_bulk_fill_row_execution_id: null,
                  last_bulk_fill_status: null,
                  last_bulk_fill_attempt_number: null,
                  latest_attempt_thread_id: "thread-1",
                  latest_attempt_state: "answer_available",
                },
                {
                  id: "row-2",
                  source_row_id: "workbook:QA:3",
                  source_row_number: 3,
                  context: "Failed context",
                  question: "Why did the latest attempt fail?",
                  current_answer: "Older answer preserved",
                  review_status: "failed",
                  approved_answer_version_id: null,
                  approved_answer_text: null,
                  last_error_detail: "Failed row detail",
                  last_bulk_fill_request_id: null,
                  last_bulk_fill_row_execution_id: null,
                  last_bulk_fill_status: "failed",
                  last_bulk_fill_attempt_number: 1,
                  latest_attempt_thread_id: "thread-failed",
                  latest_attempt_state: "failed_no_answer",
                },
              ],
              chats: [
                {
                  id: "thread-1",
                  questionnaire_row_id: "row-1",
                  title: "Row 2",
                  updated_at: "2026-03-06T10:10:00Z",
                },
                {
                  id: "thread-failed",
                  questionnaire_row_id: "row-2",
                  title: "Row 3",
                  updated_at: "2026-03-06T10:11:00Z",
                },
              ],
            }),
          );
        }

        if (url.endsWith("/api/cases/case-1/rows/row-1/answers")) {
          return new Response(
            JSON.stringify([
              {
                id: "answer-1",
                chat_thread_id: "thread-1",
                version_number: 1,
                answer_text: "Row one draft body",
                status: "draft",
                created_at: "2026-03-06T10:10:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v2",
                llm_capture_status: "captured",
                llm_request_text: "Prompt body",
                llm_response_text: "Row one draft body",
              },
            ]),
          );
        }

        if (url.endsWith("/api/cases/case-1/rows/row-2/answers")) {
          return new Response(JSON.stringify([]));
        }

        if (url.endsWith("/api/cases/case-1/threads/thread-1")) {
          return new Response(
            JSON.stringify({
              thread: {
                id: "thread-1",
                questionnaire_row_id: "row-1",
                title: "Row 2",
                updated_at: "2026-03-06T10:10:00Z",
              },
              thread_state: "answer_available",
              messages: [
                {
                  id: "message-1",
                  role: "assistant",
                  content: "Row one draft body",
                  created_at: "2026-03-06T10:10:00Z",
                },
              ],
              answer_version: {
                id: "answer-1",
                chat_thread_id: "thread-1",
                version_number: 1,
                answer_text: "Row one draft body",
                status: "draft",
                created_at: "2026-03-06T10:10:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v2",
                llm_capture_status: "captured",
                llm_request_text: "Prompt body",
                llm_response_text: "Row one draft body",
              },
              retrieval: {
                strategy_version: "retrieval.v2.hardened.v1",
                revision_mode: "initial_draft",
                revision_classifier_version: "revision_classifier.v2",
                revision_reason: "no_previous_answer_version",
                retrieval_action: "refresh_retrieval",
                retrieval_action_reason:
                  "new_or_content_change_requires_refresh",
                reused_from_retrieval_run_id: null,
                candidate_generation_mode: "sql_keyword_scope_python_rerank",
                broadened: false,
                sufficiency: "weak",
                degraded: false,
                notes: [],
                stages: [],
              },
              evidence: [
                {
                  id: "evidence-1",
                  source_kind: "case_profile_item",
                  source_label: "current_case_facts",
                  source_title: "initiative_scope",
                  excerpt: "Row one evidence",
                  score: 0.88,
                  metadata: {},
                },
              ],
              failure_detail: null,
            }),
          );
        }

        if (url.endsWith("/api/cases/case-1/threads/thread-failed")) {
          return new Response(
            JSON.stringify({
              thread: {
                id: "thread-failed",
                questionnaire_row_id: "row-2",
                title: "Row 3",
                updated_at: "2026-03-06T10:11:00Z",
              },
              thread_state: "failed_no_answer",
              messages: [
                {
                  id: "message-failed",
                  role: "user",
                  content:
                    "Draft a grounded questionnaire answer for this row.",
                  created_at: "2026-03-06T10:11:00Z",
                },
              ],
              answer_version: null,
              retrieval: {
                strategy_version: "retrieval.v2.hardened.v1",
                revision_mode: "initial_draft",
                revision_classifier_version: "revision_classifier.v2",
                revision_reason: "no_previous_answer_version",
                retrieval_action: "refresh_retrieval",
                retrieval_action_reason:
                  "new_or_content_change_requires_refresh",
                reused_from_retrieval_run_id: null,
                candidate_generation_mode: "sql_keyword_scope_python_rerank",
                broadened: false,
                sufficiency: "weak",
                degraded: false,
                notes: [],
                stages: [],
              },
              evidence: [
                {
                  id: "evidence-failed",
                  source_kind: "pdf_chunk",
                  source_label: "raw_current_pdf",
                  source_title: "page 1 chunk 1",
                  excerpt: "Failed row evidence",
                  score: 0.51,
                  metadata: {},
                },
              ],
              failure_detail: "Failed row detail",
            }),
          );
        }

        return new Response(
          JSON.stringify({ detail: `Unhandled request ${method} ${url}` }),
          { status: 500 },
        );
      },
    );

    render(<App />);

    await waitFor(() => {
      expect(screen.getAllByText("Row one draft body").length).toBeGreaterThan(
        0,
      );
    });

    fireEvent.click(
      screen.getByRole("button", {
        name: /Row 3 Why did the latest attempt fail\?/,
      }),
    );

    await waitFor(() => {
      expect(
        screen.getByText("Latest draft attempt failed"),
      ).toBeInTheDocument();
      expect(screen.getByText("Failed row detail")).toBeInTheDocument();
      expect(screen.queryAllByText("Row one draft body")).toHaveLength(0);
      expect(
        screen.getByRole("button", { name: "Approve selected version" }),
      ).toBeDisabled();
      expect(screen.getByRole("button", { name: "Reject row" })).toBeDisabled();
    });
  });

  it("retries a failed no-answer row with a fresh thread", async () => {
    let draftPayload: string | null = null;
    let drafted = false;
    fetchMock.mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        const method = init?.method ?? "GET";

        if (url.endsWith("/api/session/context")) {
          return new Response(
            JSON.stringify({
              tenant_id: "tenant-1",
              tenant_slug: "local-workspace",
              tenant_name: "Local Workspace",
              user_id: "user-1",
              user_email: "local.user.test",
              user_name: "Local Admin",
            }),
          );
        }

        if (url.endsWith("/api/cases") && method === "GET") {
          return new Response(
            JSON.stringify([
              {
                id: "case-1",
                name: "NordTransit Pilot",
                client_name: "NordTransit",
                language: "de",
                status: "active",
                created_at: "2026-03-06T10:00:00Z",
                updated_at: "2026-03-06T10:00:00Z",
              },
            ]),
          );
        }

        if (url.endsWith("/api/cases/case-1") && method === "GET") {
          return new Response(
            JSON.stringify({
              id: "case-1",
              name: "NordTransit Pilot",
              client_name: "NordTransit",
              language: "de",
              status: "active",
              created_at: "2026-03-06T10:00:00Z",
              updated_at: "2026-03-06T10:00:00Z",
              profile: null,
              latest_bulk_fill: null,
              bulk_fill_history: [],
              questionnaire_rows: [
                {
                  id: "row-1",
                  source_row_id: "workbook:QA:2",
                  source_row_number: 2,
                  context: "Failed context",
                  question: "Retry me",
                  current_answer: drafted ? "Fresh retry answer" : "",
                  review_status: drafted ? "needs_review" : "failed",
                  approved_answer_version_id: null,
                  approved_answer_text: null,
                  last_error_detail: drafted ? null : "Failed row detail",
                  last_bulk_fill_request_id: null,
                  last_bulk_fill_row_execution_id: null,
                  last_bulk_fill_status: drafted ? "drafted" : "failed",
                  last_bulk_fill_attempt_number: 1,
                  latest_attempt_thread_id: drafted
                    ? "thread-new"
                    : "thread-failed",
                  latest_attempt_state: drafted
                    ? "answer_available"
                    : "failed_no_answer",
                },
              ],
              chats: [
                {
                  id: drafted ? "thread-new" : "thread-failed",
                  questionnaire_row_id: "row-1",
                  title: "Row 2",
                  updated_at: drafted
                    ? "2026-03-06T10:12:00Z"
                    : "2026-03-06T10:10:00Z",
                },
              ],
            }),
          );
        }

        if (url.endsWith("/api/cases/case-1/rows/row-1/answers")) {
          return new Response(
            JSON.stringify(
              drafted
                ? [
                    {
                      id: "answer-1",
                      chat_thread_id: "thread-new",
                      version_number: 1,
                      answer_text: "Fresh retry answer",
                      status: "draft",
                      created_at: "2026-03-06T10:12:00Z",
                      model: "stub-ai-service",
                      generation_path: "two_stage_plan_render",
                      llm_capture_stage: "answer_rendering",
                      prompt_version: "answer_rendering_prompt.v2",
                      llm_capture_status: "captured",
                      llm_request_text: "Prompt body",
                      llm_response_text: "Fresh retry answer",
                    },
                  ]
                : [],
            ),
          );
        }

        if (url.endsWith("/api/cases/case-1/threads/thread-failed")) {
          return new Response(
            JSON.stringify({
              thread: {
                id: "thread-failed",
                questionnaire_row_id: "row-1",
                title: "Row 2",
                updated_at: "2026-03-06T10:10:00Z",
              },
              thread_state: "failed_no_answer",
              messages: [
                {
                  id: "message-failed",
                  role: "user",
                  content:
                    "Draft a grounded questionnaire answer for this row.",
                  created_at: "2026-03-06T10:10:00Z",
                },
              ],
              answer_version: null,
              retrieval: null,
              evidence: [],
              failure_detail: "Failed row detail",
            }),
          );
        }

        if (
          url.endsWith("/api/cases/case-1/rows/row-1/draft") &&
          method === "POST"
        ) {
          draftPayload = String(init?.body);
          drafted = true;
          return new Response(
            JSON.stringify({
              thread: {
                id: "thread-new",
                questionnaire_row_id: "row-1",
                title: "Row 2",
                updated_at: "2026-03-06T10:12:00Z",
              },
              messages: [
                {
                  id: "message-1",
                  role: "assistant",
                  content: "Fresh retry answer",
                  created_at: "2026-03-06T10:12:00Z",
                },
              ],
              answer_version: {
                id: "answer-1",
                chat_thread_id: "thread-new",
                version_number: 1,
                answer_text: "Fresh retry answer",
                status: "draft",
                created_at: "2026-03-06T10:12:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v2",
                llm_capture_status: "captured",
                llm_request_text: "Prompt body",
                llm_response_text: "Fresh retry answer",
              },
              retrieval: {
                strategy_version: "retrieval.v2.hardened.v1",
                revision_mode: "initial_draft",
                revision_classifier_version: "revision_classifier.v2",
                revision_reason: "no_previous_answer_version",
                retrieval_action: "refresh_retrieval",
                retrieval_action_reason:
                  "new_or_content_change_requires_refresh",
                reused_from_retrieval_run_id: null,
                candidate_generation_mode: "sql_keyword_scope_python_rerank",
                broadened: false,
                sufficiency: "weak",
                degraded: false,
                notes: [],
                stages: [],
              },
              evidence: [],
            }),
          );
        }

        if (url.endsWith("/api/cases/case-1/threads/thread-new")) {
          return new Response(
            JSON.stringify({
              thread: {
                id: "thread-new",
                questionnaire_row_id: "row-1",
                title: "Row 2",
                updated_at: "2026-03-06T10:12:00Z",
              },
              thread_state: "answer_available",
              messages: [
                {
                  id: "message-1",
                  role: "assistant",
                  content: "Fresh retry answer",
                  created_at: "2026-03-06T10:12:00Z",
                },
              ],
              answer_version: {
                id: "answer-1",
                chat_thread_id: "thread-new",
                version_number: 1,
                answer_text: "Fresh retry answer",
                status: "draft",
                created_at: "2026-03-06T10:12:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v2",
                llm_capture_status: "captured",
                llm_request_text: "Prompt body",
                llm_response_text: "Fresh retry answer",
              },
              retrieval: {
                strategy_version: "retrieval.v2.hardened.v1",
                revision_mode: "initial_draft",
                revision_classifier_version: "revision_classifier.v2",
                revision_reason: "no_previous_answer_version",
                retrieval_action: "refresh_retrieval",
                retrieval_action_reason:
                  "new_or_content_change_requires_refresh",
                reused_from_retrieval_run_id: null,
                candidate_generation_mode: "sql_keyword_scope_python_rerank",
                broadened: false,
                sufficiency: "weak",
                degraded: false,
                notes: [],
                stages: [],
              },
              evidence: [],
              failure_detail: null,
            }),
          );
        }

        return new Response(
          JSON.stringify({ detail: `Unhandled request ${method} ${url}` }),
          { status: 500 },
        );
      },
    );

    render(<App />);

    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: "Retry answer" }),
      ).toBeEnabled();
    });

    fireEvent.click(screen.getByRole("button", { name: "Retry answer" }));

    await waitFor(() => {
      expect(screen.getAllByText("Fresh retry answer")[0]).toBeInTheDocument();
    });

    expect(JSON.parse(draftPayload ?? "")).toEqual({
      message: "Generate a grounded answer for this row.",
      thread_id: null,
      revision_mode_override: null,
    });
  });

  it("loads the selected historical answer thread when the latest attempt failed", async () => {
    fetchMock.mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        const method = init?.method ?? "GET";

        if (url.endsWith("/api/session/context")) {
          return new Response(
            JSON.stringify({
              tenant_id: "tenant-1",
              tenant_slug: "local-workspace",
              tenant_name: "Local Workspace",
              user_id: "user-1",
              user_email: "local.user.test",
              user_name: "Local Admin",
            }),
          );
        }

        if (url.endsWith("/api/cases") && method === "GET") {
          return new Response(
            JSON.stringify([
              {
                id: "case-1",
                name: "NordTransit Pilot",
                client_name: "NordTransit",
                language: "de",
                status: "active",
                created_at: "2026-03-06T10:00:00Z",
                updated_at: "2026-03-06T10:00:00Z",
              },
            ]),
          );
        }

        if (url.endsWith("/api/cases/case-1") && method === "GET") {
          return new Response(
            JSON.stringify({
              id: "case-1",
              name: "NordTransit Pilot",
              client_name: "NordTransit",
              language: "de",
              status: "active",
              created_at: "2026-03-06T10:00:00Z",
              updated_at: "2026-03-06T10:00:00Z",
              profile: null,
              latest_bulk_fill: null,
              bulk_fill_history: [],
              questionnaire_rows: [
                {
                  id: "row-1",
                  source_row_id: "workbook:QA:2",
                  source_row_number: 2,
                  context: "Mixed state context",
                  question: "Show older answer safely",
                  current_answer: "Historical answer body",
                  review_status: "failed",
                  approved_answer_version_id: null,
                  approved_answer_text: null,
                  last_error_detail: "Failed row detail",
                  last_bulk_fill_request_id: null,
                  last_bulk_fill_row_execution_id: null,
                  last_bulk_fill_status: "failed",
                  last_bulk_fill_attempt_number: 1,
                  latest_attempt_thread_id: "thread-failed",
                  latest_attempt_state: "failed_no_answer",
                },
              ],
              chats: [
                {
                  id: "thread-failed",
                  questionnaire_row_id: "row-1",
                  title: "Row 2 failed",
                  updated_at: "2026-03-06T10:11:00Z",
                },
                {
                  id: "thread-old",
                  questionnaire_row_id: "row-1",
                  title: "Row 2 old",
                  updated_at: "2026-03-06T10:09:00Z",
                },
              ],
            }),
          );
        }

        if (url.endsWith("/api/cases/case-1/rows/row-1/answers")) {
          return new Response(
            JSON.stringify([
              {
                id: "answer-old",
                chat_thread_id: "thread-old",
                version_number: 1,
                answer_text: "Historical answer body",
                status: "draft",
                created_at: "2026-03-06T10:09:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v2",
                llm_capture_status: "captured",
                llm_request_text: "Prompt body",
                llm_response_text: "Historical answer body",
              },
            ]),
          );
        }

        if (url.endsWith("/api/cases/case-1/threads/thread-failed")) {
          return new Response(
            JSON.stringify({
              thread: {
                id: "thread-failed",
                questionnaire_row_id: "row-1",
                title: "Row 2 failed",
                updated_at: "2026-03-06T10:11:00Z",
              },
              thread_state: "failed_no_answer",
              messages: [
                {
                  id: "message-failed",
                  role: "user",
                  content:
                    "Draft a grounded questionnaire answer for this row.",
                  created_at: "2026-03-06T10:11:00Z",
                },
              ],
              answer_version: null,
              retrieval: null,
              evidence: [],
              failure_detail: "Failed row detail",
            }),
          );
        }

        if (url.endsWith("/api/cases/case-1/threads/thread-old")) {
          return new Response(
            JSON.stringify({
              thread: {
                id: "thread-old",
                questionnaire_row_id: "row-1",
                title: "Row 2 old",
                updated_at: "2026-03-06T10:09:00Z",
              },
              thread_state: "answer_available",
              messages: [
                {
                  id: "message-old",
                  role: "assistant",
                  content: "Historical answer body",
                  created_at: "2026-03-06T10:09:00Z",
                },
              ],
              answer_version: {
                id: "answer-old",
                chat_thread_id: "thread-old",
                version_number: 1,
                answer_text: "Historical answer body",
                status: "draft",
                created_at: "2026-03-06T10:09:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v2",
                llm_capture_status: "captured",
                llm_request_text: "Prompt body",
                llm_response_text: "Historical answer body",
              },
              retrieval: {
                strategy_version: "retrieval.v2.hardened.v1",
                revision_mode: "initial_draft",
                revision_classifier_version: "revision_classifier.v2",
                revision_reason: "no_previous_answer_version",
                retrieval_action: "refresh_retrieval",
                retrieval_action_reason:
                  "new_or_content_change_requires_refresh",
                reused_from_retrieval_run_id: null,
                candidate_generation_mode: "sql_keyword_scope_python_rerank",
                broadened: false,
                sufficiency: "weak",
                degraded: false,
                notes: [],
                stages: [],
              },
              evidence: [
                {
                  id: "evidence-old",
                  source_kind: "case_profile_item",
                  source_label: "current_case_facts",
                  source_title: "initiative_scope",
                  excerpt: "Historical evidence",
                  score: 0.88,
                  metadata: {},
                },
              ],
              failure_detail: null,
            }),
          );
        }

        return new Response(
          JSON.stringify({ detail: `Unhandled request ${method} ${url}` }),
          { status: 500 },
        );
      },
    );

    render(<App />);

    await waitFor(() => {
      expect(
        screen.getByText("Latest draft attempt failed"),
      ).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: "History" }));

    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: /Version 1/ }),
      ).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: /Version 1/ }));

    await waitFor(() => {
      expect(
        screen.getAllByText("Historical answer body")[0],
      ).toBeInTheDocument();
      expect(
        screen.getByRole("button", { name: "Approve selected version" }),
      ).toBeEnabled();
    });
  });

  it("keeps collapsed panel state across row changes but resets on remount", async () => {
    fetchMock.mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        const method = init?.method ?? "GET";

        if (url.endsWith("/api/session/context")) {
          return new Response(
            JSON.stringify({
              tenant_id: "tenant-1",
              tenant_slug: "local-workspace",
              tenant_name: "Local Workspace",
              user_id: "user-1",
              user_email: "local.user.test",
              user_name: "Local Admin",
            }),
          );
        }

        if (url.endsWith("/api/cases") && method === "GET") {
          return new Response(
            JSON.stringify([
              {
                id: "case-1",
                name: "NordTransit Pilot",
                client_name: "NordTransit",
                language: "de",
                status: "active",
                created_at: "2026-03-06T10:00:00Z",
                updated_at: "2026-03-06T10:00:00Z",
              },
            ]),
          );
        }

        if (url.endsWith("/api/cases/case-1") && method === "GET") {
          return new Response(
            JSON.stringify({
              id: "case-1",
              name: "NordTransit Pilot",
              client_name: "NordTransit",
              language: "de",
              status: "active",
              created_at: "2026-03-06T10:00:00Z",
              updated_at: "2026-03-06T10:00:00Z",
              profile: null,
              latest_bulk_fill: null,
              bulk_fill_history: [],
              questionnaire_rows: [
                {
                  id: "row-1",
                  source_row_id: "workbook:QA:2",
                  source_row_number: 2,
                  context: "Shared context",
                  question: "First question",
                  current_answer: "First answer",
                  review_status: "needs_review",
                  approved_answer_version_id: null,
                  approved_answer_text: null,
                  last_error_detail: null,
                  last_bulk_fill_request_id: null,
                  last_bulk_fill_row_execution_id: null,
                  last_bulk_fill_status: "drafted",
                  last_bulk_fill_attempt_number: 1,
                  latest_attempt_thread_id: "thread-1",
                  latest_attempt_state: "answer_available",
                },
                {
                  id: "row-2",
                  source_row_id: "workbook:QA:3",
                  source_row_number: 3,
                  context: "Secondary context",
                  question: "Second question",
                  current_answer: "Second answer",
                  review_status: "needs_review",
                  approved_answer_version_id: null,
                  approved_answer_text: null,
                  last_error_detail: null,
                  last_bulk_fill_request_id: null,
                  last_bulk_fill_row_execution_id: null,
                  last_bulk_fill_status: "drafted",
                  last_bulk_fill_attempt_number: 1,
                  latest_attempt_thread_id: "thread-2",
                  latest_attempt_state: "answer_available",
                },
              ],
              chats: [
                {
                  id: "thread-1",
                  questionnaire_row_id: "row-1",
                  title: "Row 2",
                  updated_at: "2026-03-06T10:10:00Z",
                },
                {
                  id: "thread-2",
                  questionnaire_row_id: "row-2",
                  title: "Row 3",
                  updated_at: "2026-03-06T10:11:00Z",
                },
              ],
            }),
          );
        }

        if (url.endsWith("/api/cases/case-1/rows/row-1/answers")) {
          return new Response(
            JSON.stringify([
              {
                id: "answer-1",
                chat_thread_id: "thread-1",
                version_number: 1,
                answer_text: "First answer",
                status: "draft",
                created_at: "2026-03-06T10:10:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v2",
                llm_capture_status: "captured",
                llm_request_text: "Prompt body",
                llm_response_text: "First answer",
              },
            ]),
          );
        }

        if (url.endsWith("/api/cases/case-1/rows/row-2/answers")) {
          return new Response(
            JSON.stringify([
              {
                id: "answer-2",
                chat_thread_id: "thread-2",
                version_number: 1,
                answer_text: "Second answer",
                status: "draft",
                created_at: "2026-03-06T10:11:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v2",
                llm_capture_status: "captured",
                llm_request_text: "Prompt body",
                llm_response_text: "Second answer",
              },
            ]),
          );
        }

        if (url.endsWith("/api/cases/case-1/threads/thread-1")) {
          return new Response(
            JSON.stringify({
              thread: {
                id: "thread-1",
                questionnaire_row_id: "row-1",
                title: "Row 2",
                updated_at: "2026-03-06T10:10:00Z",
              },
              thread_state: "answer_available",
              messages: [
                {
                  id: "message-1",
                  role: "user",
                  content: "Generate a grounded answer for this row.",
                  created_at: "2026-03-06T10:10:00Z",
                },
                {
                  id: "message-2",
                  role: "assistant",
                  content: "First answer",
                  created_at: "2026-03-06T10:10:10Z",
                },
              ],
              answer_version: {
                id: "answer-1",
                chat_thread_id: "thread-1",
                version_number: 1,
                answer_text: "First answer",
                status: "draft",
                created_at: "2026-03-06T10:10:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v2",
                llm_capture_status: "captured",
                llm_request_text: "Prompt body",
                llm_response_text: "First answer",
              },
              retrieval: {
                strategy_version: "retrieval.v2.hardened.v1",
                pipeline_profile_name: null,
                pipeline_config_hash: "hash-1",
                index_config_hash: "index-1",
                revision_mode: "initial_draft",
                revision_classifier_version: "revision_classifier.v2",
                revision_reason: "no_previous_answer_version",
                retrieval_action: "refresh_retrieval",
                retrieval_action_reason:
                  "new_or_content_change_requires_refresh",
                reused_from_retrieval_run_id: null,
                candidate_generation_mode: "sql_keyword_scope_python_rerank",
                broadened: false,
                sufficiency: "sufficient",
                degraded: false,
                notes: [],
                stages: [],
              },
              evidence: [
                {
                  id: "evidence-1",
                  source_kind: "case_profile_item",
                  source_label: "current_case_facts",
                  source_title: "initiative_scope",
                  excerpt: "First evidence",
                  score: 0.91,
                  metadata: {},
                },
              ],
              failure_detail: null,
            }),
          );
        }

        if (url.endsWith("/api/cases/case-1/threads/thread-2")) {
          return new Response(
            JSON.stringify({
              thread: {
                id: "thread-2",
                questionnaire_row_id: "row-2",
                title: "Row 3",
                updated_at: "2026-03-06T10:11:00Z",
              },
              thread_state: "answer_available",
              messages: [
                {
                  id: "message-3",
                  role: "user",
                  content: "Generate a grounded answer for this row.",
                  created_at: "2026-03-06T10:11:00Z",
                },
                {
                  id: "message-4",
                  role: "assistant",
                  content: "Second answer",
                  created_at: "2026-03-06T10:11:10Z",
                },
              ],
              answer_version: {
                id: "answer-2",
                chat_thread_id: "thread-2",
                version_number: 1,
                answer_text: "Second answer",
                status: "draft",
                created_at: "2026-03-06T10:11:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v2",
                llm_capture_status: "captured",
                llm_request_text: "Prompt body",
                llm_response_text: "Second answer",
              },
              retrieval: {
                strategy_version: "retrieval.v2.hardened.v1",
                pipeline_profile_name: null,
                pipeline_config_hash: "hash-2",
                index_config_hash: "index-2",
                revision_mode: "initial_draft",
                revision_classifier_version: "revision_classifier.v2",
                revision_reason: "no_previous_answer_version",
                retrieval_action: "refresh_retrieval",
                retrieval_action_reason:
                  "new_or_content_change_requires_refresh",
                reused_from_retrieval_run_id: null,
                candidate_generation_mode: "sql_keyword_scope_python_rerank",
                broadened: false,
                sufficiency: "sufficient",
                degraded: false,
                notes: [],
                stages: [],
              },
              evidence: [
                {
                  id: "evidence-2",
                  source_kind: "product_truth_chunk",
                  source_label: "product_truth",
                  source_title: "feature note",
                  excerpt: "Second evidence",
                  score: 0.82,
                  metadata: {},
                },
              ],
              failure_detail: null,
            }),
          );
        }

        return new Response(
          JSON.stringify({ detail: `Unhandled request ${method} ${url}` }),
          { status: 500 },
        );
      },
    );

    const view = render(<App />);

    await waitFor(() => {
      expect(
        screen.getByRole("button", {
          name: "Collapse retrieved evidence panel",
        }),
      ).toBeInTheDocument();
      expect(
        screen.getByRole("button", {
          name: "Collapse RfX RAG Expert sidebar",
        }),
      ).toBeInTheDocument();
    });

    fireEvent.click(
      screen.getByRole("button", {
        name: "Collapse retrieved evidence panel",
      }),
    );
    fireEvent.click(
      screen.getByRole("button", {
        name: "Collapse RfX RAG Expert sidebar",
      }),
    );

    await waitFor(() => {
      expect(
        screen.getAllByRole("button", {
          name: "Expand retrieved evidence panel",
        })[0],
      ).toBeInTheDocument();
      expect(
        screen.getByRole("button", {
          name: "Expand RfX RAG Expert sidebar",
        }),
      ).toBeInTheDocument();
    });
    expect(document.querySelector(".answer-panel")).toHaveClass(
      "chat-focus-mode",
    );

    fireEvent.click(
      screen.getByRole("button", {
        name: /Row 3 Second question needs_review drafted · attempt 1/,
      }),
    );

    await waitFor(() => {
      expect(
        screen.getByRole("heading", { name: "Row 3" }),
      ).toBeInTheDocument();
      expect(
        screen.getAllByRole("button", {
          name: "Expand retrieved evidence panel",
        })[0],
      ).toBeInTheDocument();
      expect(
        screen.getByRole("button", {
          name: "Expand RfX RAG Expert sidebar",
        }),
      ).toBeInTheDocument();
    });
    expect(document.querySelector(".answer-panel")).toHaveClass(
      "chat-focus-mode",
    );

    view.unmount();
    render(<App />);

    await waitFor(() => {
      expect(
        screen.getByRole("button", {
          name: "Collapse retrieved evidence panel",
        }),
      ).toBeInTheDocument();
      expect(
        screen.getByRole("button", {
          name: "Collapse RfX RAG Expert sidebar",
        }),
      ).toBeInTheDocument();
    });
    expect(document.querySelector(".answer-panel")).not.toHaveClass(
      "chat-focus-mode",
    );
  });

  it("keeps only the edge toggle in the collapsed sidebar rail", async () => {
    fetchMock.mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        const method = init?.method ?? "GET";

        if (url.endsWith("/api/session/context")) {
          return new Response(
            JSON.stringify({
              tenant_id: "tenant-1",
              tenant_slug: "local-workspace",
              tenant_name: "Local Workspace",
              user_id: "user-1",
              user_email: "local.user.test",
              user_name: "Local Admin",
            }),
          );
        }

        if (url.endsWith("/api/cases") && method === "GET") {
          return new Response(JSON.stringify([]));
        }

        return new Response(
          JSON.stringify({ detail: `Unhandled request ${method} ${url}` }),
          { status: 500 },
        );
      },
    );

    render(<App />);

    await waitFor(() => {
      expect(
        screen.getByRole("button", {
          name: "Collapse RfX RAG Expert sidebar",
        }),
      ).toBeInTheDocument();
      expect(screen.getByRole("link", { name: "Help" })).toBeInTheDocument();
    });

    fireEvent.click(
      screen.getByRole("button", {
        name: "Collapse RfX RAG Expert sidebar",
      }),
    );

    await waitFor(() => {
      expect(
        screen.getByRole("button", {
          name: "Expand RfX RAG Expert sidebar",
        }),
      ).toBeInTheDocument();
    });
    expect(
      screen.queryByRole("link", { name: "Help" }),
    ).not.toBeInTheDocument();
    expect(screen.queryByText("Show")).not.toBeInTheDocument();

    fireEvent.click(
      screen.getByRole("button", {
        name: "Expand RfX RAG Expert sidebar",
      }),
    );

    await waitFor(() => {
      expect(
        screen.getByRole("button", {
          name: "Collapse RfX RAG Expert sidebar",
        }),
      ).toBeInTheDocument();
      expect(screen.getByRole("link", { name: "Help" })).toBeInTheDocument();
    });
  });

  it("marks selected-case row cards as approved-stale or failed from approval and attempt state", async () => {
    fetchMock.mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        const method = init?.method ?? "GET";

        if (url.endsWith("/api/session/context")) {
          return new Response(
            JSON.stringify({
              tenant_id: "tenant-1",
              tenant_slug: "local-workspace",
              tenant_name: "Local Workspace",
              user_id: "user-1",
              user_email: "local.user.test",
              user_name: "Local Admin",
            }),
          );
        }

        if (url.endsWith("/api/cases") && method === "GET") {
          return new Response(
            JSON.stringify([
              {
                id: "case-1",
                name: "NordTransit Pilot",
                client_name: "NordTransit",
                language: "de",
                status: "active",
                created_at: "2026-03-06T10:00:00Z",
                updated_at: "2026-03-06T10:00:00Z",
              },
            ]),
          );
        }

        if (url.endsWith("/api/cases/case-1") && method === "GET") {
          return new Response(
            JSON.stringify({
              id: "case-1",
              name: "NordTransit Pilot",
              client_name: "NordTransit",
              language: "de",
              status: "active",
              created_at: "2026-03-06T10:00:00Z",
              updated_at: "2026-03-06T10:00:00Z",
              profile: null,
              latest_bulk_fill: null,
              bulk_fill_history: [],
              questionnaire_rows: [
                {
                  id: "row-1",
                  source_row_id: "workbook:QA:2",
                  source_row_number: 2,
                  context: "Shared context",
                  question: "Stale approved question",
                  current_answer: "Newer unapproved draft",
                  review_status: "needs_review",
                  approved_answer_version_id: "approved-answer-1",
                  approved_answer_text: "Approved answer body",
                  last_error_detail: null,
                  latest_attempt_state: "answer_available",
                },
                {
                  id: "row-2",
                  source_row_id: "workbook:QA:3",
                  source_row_number: 3,
                  context: "Shared context",
                  question: "Failed question",
                  current_answer: "",
                  review_status: "failed",
                  approved_answer_version_id: null,
                  approved_answer_text: null,
                  last_error_detail: "Bulk-fill failed",
                  latest_attempt_state: "failed_no_answer",
                },
              ],
              chats: [],
            }),
          );
        }

        if (
          url.endsWith("/api/cases/case-1/rows/row-1/answers") &&
          method === "GET"
        ) {
          return new Response(
            JSON.stringify([
              {
                id: "draft-answer-2",
                chat_thread_id: "thread-1",
                retrieval_run_id: "retrieval-2",
                version_number: 2,
                answer_text: "Newer unapproved draft",
                status: "draft",
                pipeline_profile_name: null,
                pipeline_config_hash: "hash-2",
                created_at: "2026-03-06T10:10:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v2",
                llm_capture_status: "captured",
                llm_request_text: "Prompt body",
                llm_response_text: "Newer unapproved draft",
              },
              {
                id: "approved-answer-1",
                chat_thread_id: "thread-1",
                retrieval_run_id: "retrieval-1",
                version_number: 1,
                answer_text: "Approved answer body",
                status: "accepted",
                pipeline_profile_name: null,
                pipeline_config_hash: "hash-1",
                created_at: "2026-03-06T10:05:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v2",
                llm_capture_status: "captured",
                llm_request_text: "Prompt body",
                llm_response_text: "Approved answer body",
              },
            ]),
          );
        }

        return new Response(
          JSON.stringify({ detail: `Unhandled request ${method} ${url}` }),
          { status: 500 },
        );
      },
    );

    render(<App />);

    const staleRow = await screen.findByRole("button", {
      name: /Stale approved question/i,
    });
    const failedRow = screen.getByRole("button", {
      name: /Failed question/i,
    });

    expect(staleRow).toHaveAttribute("data-row-visual-state", "approved-stale");
    expect(failedRow).toHaveAttribute("data-row-visual-state", "failed");
  });

  it("highlights the approved chat message and all later thread messages", async () => {
    fetchMock.mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        const method = init?.method ?? "GET";

        if (url.endsWith("/api/session/context")) {
          return new Response(
            JSON.stringify({
              tenant_id: "tenant-1",
              tenant_slug: "local-workspace",
              tenant_name: "Local Workspace",
              user_id: "user-1",
              user_email: "local.user.test",
              user_name: "Local Admin",
            }),
          );
        }

        if (url.endsWith("/api/cases") && method === "GET") {
          return new Response(
            JSON.stringify([
              {
                id: "case-1",
                name: "NordTransit Pilot",
                client_name: "NordTransit",
                language: "de",
                status: "active",
                created_at: "2026-03-06T10:00:00Z",
                updated_at: "2026-03-06T10:00:00Z",
              },
            ]),
          );
        }

        if (url.endsWith("/api/cases/case-1") && method === "GET") {
          return new Response(
            JSON.stringify({
              id: "case-1",
              name: "NordTransit Pilot",
              client_name: "NordTransit",
              language: "de",
              status: "active",
              created_at: "2026-03-06T10:00:00Z",
              updated_at: "2026-03-06T10:00:00Z",
              profile: null,
              latest_bulk_fill: null,
              bulk_fill_history: [],
              questionnaire_rows: [
                {
                  id: "row-1",
                  source_row_id: "workbook:QA:2",
                  source_row_number: 2,
                  context: "Shared context",
                  question: "Approval history question",
                  current_answer: "Later draft answer",
                  review_status: "needs_review",
                  approved_answer_version_id: "approved-answer-1",
                  approved_answer_text: "Approved answer body",
                  last_error_detail: null,
                  latest_attempt_thread_id: "thread-1",
                  latest_attempt_state: "answer_available",
                },
              ],
              chats: [
                {
                  id: "thread-1",
                  questionnaire_row_id: "row-1",
                  title: "Row 2",
                  updated_at: "2026-03-06T10:12:00Z",
                },
              ],
            }),
          );
        }

        if (
          url.endsWith("/api/cases/case-1/rows/row-1/answers") &&
          method === "GET"
        ) {
          return new Response(
            JSON.stringify([
              {
                id: "draft-answer-2",
                chat_thread_id: "thread-1",
                retrieval_run_id: "retrieval-2",
                version_number: 2,
                answer_text: "Later draft answer",
                status: "draft",
                pipeline_profile_name: null,
                pipeline_config_hash: "hash-2",
                created_at: "2026-03-06T10:12:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v2",
                llm_capture_status: "captured",
                llm_request_text: "Prompt body",
                llm_response_text: "Later draft answer",
              },
              {
                id: "approved-answer-1",
                chat_thread_id: "thread-1",
                retrieval_run_id: "retrieval-1",
                version_number: 1,
                answer_text: "Approved answer body",
                status: "accepted",
                pipeline_profile_name: null,
                pipeline_config_hash: "hash-1",
                created_at: "2026-03-06T10:05:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v2",
                llm_capture_status: "captured",
                llm_request_text: "Prompt body",
                llm_response_text: "Approved answer body",
              },
            ]),
          );
        }

        if (
          url.endsWith("/api/cases/case-1/threads/thread-1") &&
          method === "GET"
        ) {
          return new Response(
            JSON.stringify({
              thread: {
                id: "thread-1",
                questionnaire_row_id: "row-1",
                title: "Row 2",
                updated_at: "2026-03-06T10:12:00Z",
              },
              thread_state: "answer_available",
              messages: [
                {
                  id: "message-1",
                  role: "user",
                  content: "Initial request",
                  created_at: "2026-03-06T10:00:00Z",
                  answer_version_id: null,
                },
                {
                  id: "message-2",
                  role: "assistant",
                  content: "Approved answer body",
                  created_at: "2026-03-06T10:05:00Z",
                  answer_version_id: "approved-answer-1",
                },
                {
                  id: "message-3",
                  role: "user",
                  content: "Please make it shorter",
                  created_at: "2026-03-06T10:10:00Z",
                  answer_version_id: null,
                },
                {
                  id: "message-4",
                  role: "assistant",
                  content: "Later draft answer",
                  created_at: "2026-03-06T10:12:00Z",
                  answer_version_id: "draft-answer-2",
                },
              ],
              answer_version: {
                id: "draft-answer-2",
                chat_thread_id: "thread-1",
                retrieval_run_id: "retrieval-2",
                version_number: 2,
                answer_text: "Later draft answer",
                status: "draft",
                pipeline_profile_name: null,
                pipeline_config_hash: "hash-2",
                created_at: "2026-03-06T10:12:00Z",
                model: "stub-ai-service",
                generation_path: "two_stage_plan_render",
                llm_capture_stage: "answer_rendering",
                prompt_version: "answer_rendering_prompt.v2",
                llm_capture_status: "captured",
                llm_request_text: "Prompt body",
                llm_response_text: "Later draft answer",
              },
              retrieval: {
                strategy_version: "retrieval.v2.hardened.v1",
                pipeline_profile_name: null,
                pipeline_config_hash: "hash-2",
                index_config_hash: "index-2",
                revision_mode: "style_only",
                revision_classifier_version: "revision_classifier.v2",
                revision_reason: "matched_style_only_pattern",
                retrieval_action: "reuse_previous_snapshot",
                retrieval_action_reason:
                  "style_only_revision_reuses_previous_snapshot",
                reused_from_retrieval_run_id: "retrieval-1",
                candidate_generation_mode: "sql_keyword_scope_python_rerank",
                broadened: false,
                sufficiency: "sufficient",
                degraded: false,
                notes: [],
                stages: [],
              },
              evidence: [],
              failure_detail: null,
            }),
          );
        }

        return new Response(
          JSON.stringify({ detail: `Unhandled request ${method} ${url}` }),
          { status: 500 },
        );
      },
    );

    render(<App />);

    const chatLog = await screen.findByLabelText("Conversation history");
    const initialUserMessage = within(chatLog).getByText("Initial request");
    const approvedMessage = within(chatLog).getByText("Approved answer body");
    const followUpUserMessage = within(chatLog).getByText(
      "Please make it shorter",
    );
    const latestAssistantMessage = within(chatLog)
      .getAllByText("Later draft answer")
      .find((node) =>
        node.closest("article")?.hasAttribute("data-message-visual-state"),
      );

    expect(initialUserMessage.closest("article")).toHaveAttribute(
      "data-message-visual-state",
      "neutral",
    );
    expect(approvedMessage.closest("article")).toHaveAttribute(
      "data-message-visual-state",
      "approved",
    );
    expect(followUpUserMessage.closest("article")).toHaveAttribute(
      "data-message-visual-state",
      "after-approved",
    );
    expect(latestAssistantMessage?.closest("article")).toHaveAttribute(
      "data-message-visual-state",
      "after-approved",
    );
  });
});
