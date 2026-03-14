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
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.unstubAllEnvs();
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

  it("renders evidence separately from the drafted answer", async () => {
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
                  metadata: {},
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
                  metadata: {},
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

    render(<App />);

    await waitFor(() => {
      expect(screen.getByText("Questionnaire rows")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText("Generate answer"));

    await waitFor(() => {
      expect(screen.getAllByText("Draft answer body")[0]).toBeInTheDocument();
      expect(screen.getByText("Evidence excerpt")).toBeInTheDocument();
      expect(screen.getByText("Revise answer")).toBeInTheDocument();
    });

    expect(screen.queryByText("Data browser")).not.toBeInTheDocument();
    expect(screen.queryByDisplayValue("Prompt body")).not.toBeInTheDocument();
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

    expect(screen.getByText("Data browser")).toBeInTheDocument();
    expect(screen.getByDisplayValue("Prompt body")).toBeInTheDocument();
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
        screen.getByPlaceholderText(
          "Ask for a style revision such as shorter, clearer, or more formal.",
        ),
      ).toBeEnabled();
      expect(
        screen.getByRole("button", { name: "Revise answer" }),
      ).toBeDisabled();
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
      expect(screen.getByText("Revise answer")).toBeInTheDocument();
    });
    const chatLog = screen.getByLabelText("Conversation history");
    Object.defineProperty(chatLog, "scrollHeight", {
      configurable: true,
      get: () => 320,
    });
    chatLog.scrollTop = 0;

    fireEvent.change(
      screen.getByPlaceholderText(
        "Ask for a style revision such as shorter, clearer, or more formal.",
      ),
      {
        target: {
          value: "ok but remove the bullet points. just a flowing text",
        },
      },
    );
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
      expect(screen.getAllByText("needs_review")[0]).toBeInTheDocument();
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
        screen.getByRole("button", {
          name: "Expand retrieved evidence panel",
        }),
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
        screen.getByRole("button", {
          name: "Expand retrieved evidence panel",
        }),
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

  it("shows only Show in the collapsed sidebar rail", async () => {
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
    const latestAssistantMessage =
      within(chatLog).getByText("Later draft answer");

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
    expect(latestAssistantMessage.closest("article")).toHaveAttribute(
      "data-message-visual-state",
      "after-approved",
    );
  });
});
