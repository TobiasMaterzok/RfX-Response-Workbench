import { useMemo, useState } from "react";

import "./styles.css";
import "./help.css";

type HelpStage = {
  id: string;
  title: string;
  summary: string;
  flowLabel: string;
};

type HelpNode = {
  id: string;
  column: string;
  eyebrow: string;
  title: string;
  summary: string;
  bullets: string[];
  touchpoints: string[];
  stageIds: string[];
};

const HELP_STAGES: HelpStage[] = [
  {
    id: "scope",
    title: "Scope",
    flowLabel: "Scoped source artifacts",
    summary:
      "Identity headers, tenant/user resolution, and case scope decide which uploads, rows, threads, and jobs the request can see.",
  },
  {
    id: "ingest",
    title: "Ingest",
    flowLabel: "Indexed evidence corpora",
    summary:
      "A live case creates uploads, PDF evidence, case-profile items, and questionnaire rows; historical exemplars and product truth stay separate corpora.",
  },
  {
    id: "retrieve",
    title: "Retrieve",
    flowLabel: "Retrieval snapshot",
    summary:
      "Retrieval builds quota-based candidate pools per corpus and persists a RetrievalRun plus ranked RetrievalSnapshotItems.",
  },
  {
    id: "generate",
    title: "Generate",
    flowLabel: "Plan + render",
    summary:
      "The app stages generation into packed evidence, AnswerPlan, rendered answer text, AnswerVersion persistence, and EvidenceLinks.",
  },
  {
    id: "review",
    title: "Review",
    flowLabel: "Explicit review state",
    summary:
      "Attempt state, answer history, and row review state are separate concepts; approval is a pointer on QuestionnaireRow, not on the thread.",
  },
  {
    id: "export",
    title: "Export",
    flowLabel: "Deterministic export",
    summary:
      "Export selects either approved answers or latest available answers and renders aligned XLSX, CSV, and ZIP artifacts from the same resolved row mapping.",
  },
  {
    id: "lineage",
    title: "Lineage",
    flowLabel: "Pipeline + reproducibility lineage",
    summary:
      "Pipeline hashes, execution runs, artifact builds, repo/runtime snapshots, and model invocations make the workflow reproducible and debuggable.",
  },
];

const HELP_NODES: HelpNode[] = [
  {
    id: "identity-scope",
    column: "scope",
    eyebrow: "Identity and scope",
    title: "Tenant / user / membership gate every request",
    summary:
      "The frontend is a tenant-scoped workspace. Headers resolve a user inside a tenant and every case, row, thread, and export is checked against that scope.",
    bullets: [
      "Membership is unique per tenant/user pair.",
      "Headers `X-Tenant-Slug` and `X-User-Email` are the live scope boundary.",
      "Case, row, and thread lookup all re-check tenant scope before use.",
    ],
    touchpoints: [
      "backend/app/api/deps.py",
      "backend/app/services/identity.py",
      "backend/app/models/entities.py",
    ],
    stageIds: ["scope"],
  },
  {
    id: "live-case",
    column: "scope",
    eyebrow: "Live case domain",
    title: "Case ingestion turns uploads into row-level work",
    summary:
      "A case is not just a PDF. It becomes uploads, PDF chunks, a structured case profile, a questionnaire, and row-level review state.",
    bullets: [
      "Questionnaire row ids are deterministic from file/sheet/row number.",
      "Case profile JSON is canonical; CaseProfileItem is the searchable projection.",
      "Questionnaire review state lives on QuestionnaireRow.",
    ],
    touchpoints: [
      "backend/app/services/cases.py",
      "backend/app/services/workbooks.py",
      "backend/app/services/case_profiles.py",
    ],
    stageIds: ["scope", "ingest"],
  },
  {
    id: "evidence-corpora",
    column: "corpora",
    eyebrow: "Evidence corpora",
    title: "The system retrieves from four separate evidence pools",
    summary:
      "Live case facts, raw current PDF chunks, approved product truth, and approved historical exemplars are kept separate all the way through retrieval and frontend grouping.",
    bullets: [
      "Historical exemplar rows are not canonical product truth.",
      "Product truth chunks are canonical vendor claims.",
      "Raw current PDF evidence is retrieved separately from structured case facts.",
    ],
    touchpoints: [
      "backend/app/services/pdf_chunks.py",
      "backend/app/services/product_truth.py",
      "backend/app/services/seed.py",
    ],
    stageIds: ["ingest", "retrieve"],
  },
  {
    id: "retrieval-runtime",
    column: "retrieve",
    eyebrow: "Retrieval runtime",
    title: "Quota-based retrieval produces a persisted snapshot",
    summary:
      "Retrieval is not a single top-k query. It builds corpus-specific candidate pools, ranks them, applies corpus quotas, and persists the chosen snapshot.",
    bullets: [
      "RetrievalRun stores query text and request context.",
      "RetrievalSnapshotItem keeps ranked evidence with source identity and score.",
      "Revision intent can influence scoring for content-changing revisions.",
    ],
    touchpoints: [
      "backend/app/services/retrieval.py",
      "backend/app/models/entities.py",
    ],
    stageIds: ["retrieve", "lineage"],
  },
  {
    id: "conversation-state",
    column: "generate",
    eyebrow: "Conversation state",
    title: "Thread, attempt state, and answer history are deliberately separate",
    summary:
      "A ChatThread can contain messages and retrieval evidence even when no AnswerVersion exists. That is expected lineage, not broken data.",
    bullets: [
      "`pending_no_answer`, `failed_no_answer`, and `answer_available` describe attempt state.",
      "Answer history is the set of persisted AnswerVersions for a row.",
      "Review state is a separate row-level concept again.",
    ],
    touchpoints: [
      "backend/app/api/routers/cases.py",
      "backend/app/services/answers.py",
      "frontend/src/App.tsx",
    ],
    stageIds: ["generate", "review"],
  },
  {
    id: "answer-generation",
    column: "generate",
    eyebrow: "Generation path",
    title: "Plan first, then render, then persist answer lineage",
    summary:
      "The primary path is retrieval snapshot -> normalized and packed evidence -> AnswerPlan -> rendered answer -> AnswerVersion + EvidenceLinks.",
    bullets: [
      "Historical examples are style/pattern context, not product authority.",
      "Style-only revision reuses prior retrieval and planning lineage.",
      "Regenerate answer is a forced content-change path, not an in-place overwrite.",
    ],
    touchpoints: [
      "backend/app/services/answer_prompting.py",
      "backend/app/services/answers.py",
      "backend/app/prompts/answer_planning.py",
      "backend/app/prompts/answer_rendering.py",
    ],
    stageIds: ["generate", "review", "lineage"],
  },
  {
    id: "review-export",
    column: "review",
    eyebrow: "Review and export",
    title: "Approval stays on the row, exports stay deterministic",
    summary:
      "Approving a row stores an explicit approved_answer_version_id on QuestionnaireRow. Export then resolves one answer selection pass for XLSX, CSV, and ZIP.",
    bullets: [
      "Approval is a row pointer, not an AnswerVersion flag alone.",
      "`approved_only` and `latest_available` are explicit export modes.",
      "Latest available and approved-only exports both emit deterministic placeholders when needed.",
    ],
    touchpoints: [
      "backend/app/services/exports.py",
      "backend/app/api/routers/cases.py",
    ],
    stageIds: ["review", "export"],
  },
  {
    id: "bulk-fill",
    column: "review",
    eyebrow: "Bulk fill",
    title: "Bulk fill is a durable job system, not a loop",
    summary:
      "Bulk fill persists request roots, per-row executions, lifecycle events, worker claims, and the same row-drafting path used by manual generation.",
    bullets: [
      "Approved rows are excluded or skipped to preserve approvals.",
      "Bulk fill drafts with `thread=None`, so each attempt starts a fresh thread.",
      "Operational attempt history never silently replaces approved content.",
    ],
    touchpoints: [
      "backend/app/services/bulk_fill.py",
      "backend/app/services/answers.py",
    ],
    stageIds: ["review", "export", "lineage"],
  },
  {
    id: "pipeline-lineage",
    column: "lineage",
    eyebrow: "Pipeline lineage",
    title: "Runtime behavior is anchored to resolved config hashes",
    summary:
      "The app persists resolved pipeline config, index config, runtime config, and compatibility hashes so retrieval and generation runs can be traced to exact settings.",
    bullets: [
      "Index-time and runtime config are conceptually separate.",
      "Cases and builds store compatibility hashes explicitly.",
      "Pipeline compatibility is checked before reuse.",
    ],
    touchpoints: [
      "backend/app/pipeline/config.py",
      "backend/app/models/entities.py",
    ],
    stageIds: ["lineage"],
  },
  {
    id: "reproducibility",
    column: "lineage",
    eyebrow: "Reproducibility lineage",
    title: "Execution runs and model invocations are first-class artifacts",
    summary:
      "Repo snapshots, runtime snapshots, source manifests, execution runs, artifact builds, and model invocations are persisted to make the workflow auditable and replayable.",
    bullets: [
      "Run types cover retrieval, row draft/revision, bulk fill, import, and export.",
      "Planning-stage payloads live in ModelInvocation even when answer versions store render-stage text directly.",
      "Strict-eval mode enforces consistency checks across the lineage graph.",
    ],
    touchpoints: [
      "backend/app/services/reproducibility.py",
      "backend/app/models/entities.py",
    ],
    stageIds: ["lineage"],
  },
];

const DIAGRAM_COLUMNS = [
  { id: "scope", title: "Scope and source" },
  { id: "corpora", title: "Evidence corpora" },
  { id: "retrieve", title: "Retrieval snapshot" },
  { id: "generate", title: "Conversation and generation" },
  { id: "review", title: "Review and operations" },
  { id: "lineage", title: "Pipeline and reproducibility" },
];

function CodeModelHelpPage() {
  const [selectedStageId, setSelectedStageId] = useState<string>("generate");
  const [selectedNodeId, setSelectedNodeId] = useState<string>("answer-generation");

  const selectedStage = useMemo(
    () => HELP_STAGES.find((stage) => stage.id === selectedStageId) ?? HELP_STAGES[0],
    [selectedStageId],
  );
  const selectedNode = useMemo(
    () => HELP_NODES.find((node) => node.id === selectedNodeId) ?? HELP_NODES[0],
    [selectedNodeId],
  );

  function handleStageSelect(stageId: string) {
    setSelectedStageId(stageId);
    const firstNodeForStage = HELP_NODES.find((node) => node.stageIds.includes(stageId));
    if (firstNodeForStage) {
      setSelectedNodeId(firstNodeForStage.id);
    }
  }

  return (
    <div className="help-shell">
      <header className="help-hero panel">
        <div>
          <p className="eyebrow">Help</p>
          <h1>Conceptual Code Model</h1>
          <p className="help-hero-copy">
            This page turns the repo&apos;s conceptual model into a navigable flow:
            source scope, evidence corpora, retrieval snapshot, staged generation,
            explicit review state, and the lineage layers underneath it all.
          </p>
        </div>
        <div className="help-hero-actions">
          <a className="panel-toggle" href="/">
            Back to workspace
          </a>
        </div>
      </header>

      <section className="help-thesis panel">
        <p className="eyebrow">Core thesis</p>
        <div className="help-thesis-flow">
          {HELP_STAGES.map((stage) => (
            <button
              key={stage.id}
              type="button"
              className={
                stage.id === selectedStageId ? "help-stage active" : "help-stage"
              }
              onClick={() => handleStageSelect(stage.id)}
            >
              <span>{stage.title}</span>
              <strong>{stage.flowLabel}</strong>
            </button>
          ))}
        </div>
        <p className="help-stage-summary">{selectedStage.summary}</p>
      </section>

      <section className="help-diagram-layout">
        <div className="help-diagram panel">
          <div className="help-diagram-header">
            <div>
              <p className="eyebrow">Interactive flow diagram</p>
              <h2>How the system actually thinks</h2>
            </div>
            <p className="status-line">
              Click a stage or node to inspect the working assumptions the code
              relies on.
            </p>
          </div>

          <div className="help-diagram-columns">
            {DIAGRAM_COLUMNS.map((column) => (
              <section key={column.id} className="help-diagram-column">
                <div className="help-column-heading">
                  <span>{column.title}</span>
                </div>
                {HELP_NODES.filter((node) => node.column === column.id).map((node) => {
                  const matchesStage = node.stageIds.includes(selectedStageId);
                  const isActive = node.id === selectedNodeId;
                  return (
                    <button
                      key={node.id}
                      type="button"
                      className={
                        isActive
                          ? "help-node-card active"
                          : matchesStage
                            ? "help-node-card linked"
                            : "help-node-card"
                      }
                      onClick={() => setSelectedNodeId(node.id)}
                    >
                      <span>{node.eyebrow}</span>
                      <strong>{node.title}</strong>
                      <p>{node.summary}</p>
                    </button>
                  );
                })}
              </section>
            ))}
          </div>
        </div>

        <aside className="help-detail panel">
          <div className="help-detail-top">
            <p className="eyebrow">Selected node</p>
            <h2>{selectedNode.title}</h2>
            <p className="help-detail-summary">{selectedNode.summary}</p>
          </div>

          <div className="help-tag-row">
            {selectedNode.stageIds.map((stageId) => {
              const stage = HELP_STAGES.find((item) => item.id === stageId);
              return (
                <span key={stageId} className="help-tag">
                  {stage?.title ?? stageId}
                </span>
              );
            })}
          </div>

          <section className="help-detail-section">
            <h3>Key invariants</h3>
            <ul className="help-bullet-list">
              {selectedNode.bullets.map((bullet) => (
                <li key={bullet}>{bullet}</li>
              ))}
            </ul>
          </section>

          <section className="help-detail-section">
            <h3>Code touchpoints</h3>
            <div className="help-touchpoints">
              {selectedNode.touchpoints.map((item) => (
                <code key={item}>{item}</code>
              ))}
            </div>
          </section>

          <section className="help-detail-section">
            <h3>Why this matters in the UI</h3>
            <p>
              The frontend is intentionally projecting several separate backend
              concepts at once: row review state, latest attempt state, answer
              history, retrieval evidence, and thread detail. This page is meant
              to make those separations legible before you change behavior.
            </p>
          </section>
        </aside>
      </section>

      <section className="help-callouts">
        <article className="panel help-callout-card">
          <p className="eyebrow">State separation</p>
          <h2>Attempt state is not answer history is not review state</h2>
          <div className="help-callout-grid">
            <div>
              <strong>Attempt state</strong>
              <p>
                What happened in the latest drafting attempt for the row.
              </p>
            </div>
            <div>
              <strong>Answer history</strong>
              <p>
                Which persisted AnswerVersions exist for the row across threads.
              </p>
            </div>
            <div>
              <strong>Review state</strong>
              <p>
                Which answer export and approval logic currently treats as live.
              </p>
            </div>
          </div>
        </article>

        <article className="panel help-callout-card">
          <p className="eyebrow">Revision contract</p>
          <h2>Regenerate, revise, and retry are different code paths</h2>
          <div className="help-callout-grid">
            <div>
              <strong>Regenerate answer</strong>
              <p>
                Forced `content_change`: refresh retrieval, build a new plan,
                persist a new AnswerVersion.
              </p>
            </div>
            <div>
              <strong>Revise answer</strong>
              <p>
                Forced `style_only`: reuse prior retrieval snapshot and planning
                lineage, then render again.
              </p>
            </div>
            <div>
              <strong>Retry failed attempt</strong>
              <p>
                Fresh draft request with `thread_id=null`, not a reuse path for
                missing output.
              </p>
            </div>
          </div>
        </article>
      </section>
    </div>
  );
}

export default CodeModelHelpPage;
