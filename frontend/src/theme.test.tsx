import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";

import App from "./App";
import CodeModelHelpPage from "./CodeModelHelpPage";
import ThemeToggle from "./ThemeToggle";
import {
  THEME_STORAGE_KEY,
  type ThemePreference,
  useThemePreference,
} from "./theme";

type MatchMediaController = {
  setMatches: (nextMatches: boolean) => void;
};

function installMatchMedia(initialMatches: boolean): MatchMediaController {
  const listeners = new Set<(event: MediaQueryListEvent) => void>();
  const mediaQueryList = {
    matches: initialMatches,
    media: "(prefers-color-scheme: dark)",
    onchange: null,
    addEventListener: vi.fn(
      (eventName: string, listener: EventListenerOrEventListenerObject) => {
        if (eventName !== "change") {
          return;
        }
        if (typeof listener === "function") {
          listeners.add(listener as (event: MediaQueryListEvent) => void);
        }
      },
    ),
    removeEventListener: vi.fn(
      (eventName: string, listener: EventListenerOrEventListenerObject) => {
        if (eventName !== "change") {
          return;
        }
        if (typeof listener === "function") {
          listeners.delete(listener as (event: MediaQueryListEvent) => void);
        }
      },
    ),
    addListener: vi.fn((listener: (event: MediaQueryListEvent) => void) => {
      listeners.add(listener);
    }),
    removeListener: vi.fn((listener: (event: MediaQueryListEvent) => void) => {
      listeners.delete(listener);
    }),
    dispatchEvent: vi.fn(() => true),
  } as MediaQueryList;

  const matchMedia = vi.fn().mockImplementation(() => mediaQueryList);
  Object.defineProperty(window, "matchMedia", {
    configurable: true,
    writable: true,
    value: matchMedia,
  });

  return {
    setMatches(nextMatches) {
      (mediaQueryList as MediaQueryList & { matches: boolean }).matches =
        nextMatches;
      const event = {
        matches: nextMatches,
        media: mediaQueryList.media,
      } as MediaQueryListEvent;
      listeners.forEach((listener) => listener(event));
      mediaQueryList.onchange?.(event);
    },
  };
}

function ThemeHarness() {
  const { preference, setPreference } = useThemePreference();

  return <ThemeToggle preference={preference} onChange={setPreference} />;
}

function sessionContext() {
  return {
    tenant_id: "tenant-1",
    tenant_slug: "local-workspace",
    tenant_name: "Local Workspace",
    user_id: "user-1",
    user_email: "local.user@example.test",
    user_name: "Local Admin",
  };
}

function questionnaireRow(overrides?: Record<string, unknown>) {
  return {
    id: "row-1",
    source_row_id: "workbook:QA:2",
    source_row_number: 2,
    context: "Shared context",
    question: "What fits the scope?",
    current_answer: "Approved answer draft",
    review_status: "approved",
    approved_answer_version_id: "answer-1",
    approved_answer_text: "Approved answer draft",
    last_error_detail: null,
    last_bulk_fill_request_id: null,
    last_bulk_fill_row_execution_id: null,
    last_bulk_fill_status: null,
    last_bulk_fill_attempt_number: null,
    latest_attempt_thread_id: "thread-1",
    latest_attempt_state: "answer_available",
    ...overrides,
  };
}

function installAppFetchMock() {
  const fetchMock = vi.fn(
    async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      const method = init?.method ?? "GET";

      if (url.endsWith("/api/session/context")) {
        return new Response(JSON.stringify(sessionContext()));
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
              questionnaireRow(),
              questionnaireRow({
                id: "row-2",
                source_row_id: "workbook:QA:3",
                source_row_number: 3,
                question: "What failed?",
                current_answer: "",
                review_status: "failed",
                approved_answer_version_id: null,
                approved_answer_text: null,
                latest_attempt_thread_id: null,
                latest_attempt_state: "failed_no_answer",
              }),
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
              id: "answer-2",
              chat_thread_id: "thread-1",
              retrieval_run_id: "retrieval-2",
              version_number: 2,
              answer_text: "Updated answer draft",
              status: "draft",
              pipeline_profile_name: null,
              pipeline_config_hash: "pipeline-2",
              created_at: "2026-03-06T10:12:00Z",
              model: "stub-ai-service",
              generation_path: "render_only_reuse_plan",
              llm_capture_stage: "answer_rendering",
              prompt_version: "answer_rendering_prompt.v2",
              llm_capture_status: "captured",
              llm_request_text: "Prompt body 2",
              llm_response_text: "Updated answer draft",
            },
            {
              id: "answer-1",
              chat_thread_id: "thread-1",
              retrieval_run_id: "retrieval-1",
              version_number: 1,
              answer_text: "Approved answer draft",
              status: "draft",
              pipeline_profile_name: null,
              pipeline_config_hash: "pipeline-1",
              created_at: "2026-03-06T10:10:00Z",
              model: "stub-ai-service",
              generation_path: "two_stage_plan_render",
              llm_capture_stage: "answer_rendering",
              prompt_version: "answer_rendering_prompt.v2",
              llm_capture_status: "captured",
              llm_request_text: "Prompt body 1",
              llm_response_text: "Approved answer draft",
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
                role: "assistant",
                content: "Approved answer draft",
                created_at: "2026-03-06T10:10:00Z",
                answer_version_id: "answer-1",
              },
              {
                id: "message-2",
                role: "assistant",
                content: "Updated answer draft",
                created_at: "2026-03-06T10:12:00Z",
                answer_version_id: "answer-2",
              },
            ],
            answer_version: {
              id: "answer-2",
              chat_thread_id: "thread-1",
              retrieval_run_id: "retrieval-2",
              version_number: 2,
              answer_text: "Updated answer draft",
              status: "draft",
              pipeline_profile_name: null,
              pipeline_config_hash: "pipeline-2",
              created_at: "2026-03-06T10:12:00Z",
              model: "stub-ai-service",
              generation_path: "render_only_reuse_plan",
              llm_capture_stage: "answer_rendering",
              prompt_version: "answer_rendering_prompt.v2",
              llm_capture_status: "captured",
              llm_request_text: "Prompt body 2",
              llm_response_text: "Updated answer draft",
            },
            retrieval: null,
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

  vi.stubGlobal("fetch", fetchMock);
}

describe("theme support", () => {
  beforeEach(() => {
    window.localStorage.clear();
    delete document.documentElement.dataset.theme;
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    window.localStorage.clear();
    delete document.documentElement.dataset.theme;
  });

  it("defaults to system preference and resolves from the OS theme", async () => {
    installMatchMedia(true);

    render(<ThemeHarness />);

    await waitFor(() => {
      expect(document.documentElement.dataset.theme).toBe("dark");
    });
    expect(screen.getByRole("button", { name: "Auto" })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    expect(window.localStorage.getItem(THEME_STORAGE_KEY)).toBe("system");
  });

  it.each<ThemePreference>(["light", "dark"])(
    "restores a stored %s preference",
    async (preference) => {
      installMatchMedia(preference === "dark");
      window.localStorage.setItem(THEME_STORAGE_KEY, preference);

      render(<ThemeHarness />);

      await waitFor(() => {
        expect(document.documentElement.dataset.theme).toBe(preference);
      });
      expect(
        screen.getByRole("button", {
          name: preference === "light" ? "Light" : "Dark",
        }),
      ).toHaveAttribute("aria-pressed", "true");
    },
  );

  it("updates the persisted preference and root dataset from the toggle", async () => {
    installMatchMedia(false);

    render(<ThemeHarness />);

    fireEvent.click(screen.getByRole("button", { name: "Dark" }));
    await waitFor(() => {
      expect(document.documentElement.dataset.theme).toBe("dark");
    });
    expect(window.localStorage.getItem(THEME_STORAGE_KEY)).toBe("dark");

    fireEvent.click(screen.getByRole("button", { name: "Light" }));
    await waitFor(() => {
      expect(document.documentElement.dataset.theme).toBe("light");
    });
    expect(window.localStorage.getItem(THEME_STORAGE_KEY)).toBe("light");
  });

  it("reacts to OS changes only while in system mode", async () => {
    const mediaQuery = installMatchMedia(false);

    render(<ThemeHarness />);

    await waitFor(() => {
      expect(document.documentElement.dataset.theme).toBe("light");
    });

    await act(async () => {
      mediaQuery.setMatches(true);
    });
    await waitFor(() => {
      expect(document.documentElement.dataset.theme).toBe("dark");
    });

    fireEvent.click(screen.getByRole("button", { name: "Light" }));
    await waitFor(() => {
      expect(document.documentElement.dataset.theme).toBe("light");
    });

    await act(async () => {
      mediaQuery.setMatches(false);
    });
    await waitFor(() => {
      expect(document.documentElement.dataset.theme).toBe("light");
    });

    fireEvent.click(screen.getByRole("button", { name: "Auto" }));
    await waitFor(() => {
      expect(document.documentElement.dataset.theme).toBe("light");
    });

    await act(async () => {
      mediaQuery.setMatches(true);
    });
    await waitFor(() => {
      expect(document.documentElement.dataset.theme).toBe("dark");
    });
  });

  it("renders the theme control on the workspace and preserves row/message states", async () => {
    installMatchMedia(false);
    installAppFetchMock();
    window.localStorage.setItem(THEME_STORAGE_KEY, "dark");

    render(<App />);

    await waitFor(() => {
      expect(screen.getByText("Questionnaire rows")).toBeInTheDocument();
    });

    expect(document.documentElement.dataset.theme).toBe("dark");
    expect(screen.getByRole("button", { name: "Dark" })).toHaveAttribute(
      "aria-pressed",
      "true",
    );

    const approvedRowButton = screen
      .getAllByText("What fits the scope?")
      .map((node) => node.closest("button"))
      .find((button) => button?.hasAttribute("data-row-visual-state"));
    expect(approvedRowButton).toHaveAttribute(
      "data-row-visual-state",
      "approved",
    );
    expect(screen.getByText("What failed?").closest("button")).toHaveAttribute(
      "data-row-visual-state",
      "failed",
    );
    const chatLog = screen.getByLabelText("Conversation history");
    expect(
      within(chatLog).getByText("Approved answer draft").closest("article"),
    ).toHaveAttribute("data-message-visual-state", "approved");
    expect(
      within(chatLog).getByText("Updated answer draft").closest("article"),
    ).toHaveAttribute("data-message-visual-state", "after-approved");
  });

  it("renders the same theme control on the help page", async () => {
    installMatchMedia(false);

    render(<CodeModelHelpPage />);

    expect(screen.getByRole("button", { name: "Auto" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Light" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Dark" })).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Back to workspace" }),
    ).toBeInTheDocument();
  });
});
