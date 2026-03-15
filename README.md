# RfX RAG Expert

RfX RAG Expert is a case-scoped RfX drafting workspace for proposal and bid teams. It ingests a client PDF and questionnaire workbook, extracts structured case facts, retrieves from approved product truth and historical exemplars, drafts grounded answers row by row, and keeps review and export explicit.

The repository includes the FastAPI backend, the React frontend, sample synthetic seed data, and the docs that describe the app's data contracts, retrieval model, and reproducibility layer.

The screenshots below use mocked sample data only. No customer files or local filesystem paths are shown.

## Interface

<p align="center">
  <img src="docs/screenshots/workspace-light.png" alt="RfX RAG Expert workspace in light mode" width="49%" />
  <img src="docs/screenshots/workspace-dark.png" alt="RfX RAG Expert workspace in dark mode" width="49%" />
</p>

## Key Capabilities

- Create a case from a client PDF and optional questionnaire workbook.
- Extract a structured case profile from the PDF.
- Retrieve evidence from the current case, a product-truth corpus, and approved historical Q&A exemplars.
- Draft and revise answers for individual questionnaire rows.
- Run bulk-fill through the same grounded drafting path.
- Keep reviewer approval explicit before export.
- Export filled questionnaire artifacts as a ZIP bundle containing XLSX and CSV.

## Repo Layout

- `backend/`: FastAPI app, services, models, CLI, and Alembic migrations
- `frontend/`: React + TypeScript UI
- `docs/`: contracts and advanced architecture/runtime docs
- `seed_data/`: synthetic sample vendor truth and customer exemplar packages

## Public Repo Notes

- The tracked seed data is synthetic and safe for public demo use.
- The default local identity headers in `frontend/.env.local` are a development convenience only, not a production auth model.
- The app currently targets local development and evaluation workflows first; production deployment concerns such as real auth, secret management, and hosted infrastructure are intentionally out of scope for this repo.

## Requirements

- Python 3.12+
- Node.js 20+
- PostgreSQL 16+ with the `vector` extension available
- `LLM_API_KEY` for extraction, embeddings, answer generation, and seed import
- PowerShell 5.1+ or PowerShell 7+ for the Win11 helper script

## Quick Start On Windows 11

This repo supports a native Win11 local run path for the app itself. PostgreSQL 16 and `pgvector` are still explicit prerequisites.

Start with the Win11 prerequisite guide:

- [docs/windows-local-setup.md](docs/windows-local-setup.md)
- [docs/docker-compose-setup.md](docs/docker-compose-setup.md) if you want the backend, frontend, worker, and PostgreSQL to run inside Linux containers instead of a native Win11 Python/Node setup

### 1. Bootstrap the repo

This creates `.venv`, installs the backend/frontend dependencies, and copies `.env` plus `frontend/.env.local` if they do not already exist.

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\windows\dev.ps1 bootstrap
```

If you want to return to the pre-bootstrap repo state for the repo-owned artifacts only, run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\windows\dev.ps1 hard-reset
```

This removes `.venv`, `frontend\node_modules`, repo `.env`, `frontend\.env.local`, and backend `*.egg-info`. It does not remove PostgreSQL data, Docker volumes, or app storage.

### 2. Configure environment

Edit the generated config files and set at least these repo-root `.env` values:

- `RFX_DATABASE_URL`
- `RFX_STORAGE_ROOT`
- `LLM_API_KEY`

The backend, Alembic, and CLI commands read the repo-root `.env` automatically.

### 3. Initialize the database and local identity

This validates PostgreSQL reachability, creates the target database if missing, verifies that the `vector` extension is available, applies the migration, and creates the local tenant/user.

For the native Win11 helper path, this can target either:

- PostgreSQL running directly on Windows
- PostgreSQL + `pgvector` running in Docker and exposed to Windows via `localhost`

If you are using the full Docker Compose app stack below, skip this helper step because the Compose `init` service does it for you.

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\windows\dev.ps1 init-db
```

### 4. Load a corpus

Sample/demo path:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\windows\dev.ps1 seed-sample
```

Use that helper only for the native or hybrid Win11 path where the backend and worker also run from the host repo checkout. If you are running the full Docker Compose app stack, seed through the backend container instead:

```bash
docker compose exec backend python -m app.cli import-historical-corpus
docker compose exec backend python -m app.cli import-product-truth
```

Real-customer/private corpus path:

Historical data can be loaded from any corpus root that matches the expected manifest and folder layout. One local private example is `seed_data\local\`. The supported Win11 commands are:

```powershell
.\.venv\Scripts\python.exe -m app.cli import-historical-corpus --base-path <private-corpus-root>
.\.venv\Scripts\python.exe -m app.cli reimport-product-truth --path <private-corpus-root>\product_truth\product_truth.json
```

When replacing an existing product-truth corpus, use `reimport-product-truth` rather than additive `import-product-truth`.

### 5. Run the services

Backend:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\windows\dev.ps1 run-backend
```

Frontend, in a second terminal:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\windows\dev.ps1 run-frontend
```

Bulk-fill worker, in a third terminal if you want queued jobs to run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\windows\dev.ps1 run-worker
```

The backend listens on `http://127.0.0.1:8000` and the Vite frontend on `http://127.0.0.1:5173`.

## Docker Compose On Win11/macOS/Linux

This repo also supports a containerized local run path built around Linux containers for PostgreSQL + `pgvector`, the FastAPI backend, the Vite frontend, and the bulk-fill worker.
This is the better path on locked-down Win11 systems where host `npm` is restricted.

Quick path:

```bash
cp .env.example .env
docker pull pgvector/pgvector:pg18-trixie
docker compose up --build -d postgres backend frontend
docker compose exec backend python -m app.cli import-historical-corpus
docker compose exec backend python -m app.cli import-product-truth
docker compose up -d worker
```

Open the UI from the host browser at `http://127.0.0.1:5173`.

The full container guide, including Win11 notes and volume behavior, lives in [docs/docker-compose-setup.md](docs/docker-compose-setup.md).

## Quick Start On macOS/Linux

This first public release assumes a fresh PostgreSQL database. It does **not** support upgrading an older local/internal schema chain.

### 1. Install dependencies

This creates a local Python environment and installs the backend/frontend dependencies used by the repo.

```bash
python3 -m venv .venv
source .venv/bin/activate
make install
```

### 2. Prepare PostgreSQL

This ensures a local PostgreSQL instance is running and that the database named in `RFX_DATABASE_URL` exists before Alembic connects.

For the default `.env.example`, create the default local database:

```bash
psql 'postgresql://postgres:postgres@localhost:5432/postgres' -c "CREATE DATABASE rfx_rag_expert;"
```

If you change `RFX_DATABASE_URL`, create that database name instead. Your PostgreSQL installation must also have the `vector` extension available.

### 3. Configure environment

This writes the local backend and frontend runtime settings the app will read when you run commands and start the UI.

```bash
cp .env.example .env
cp frontend/.env.example frontend/.env.local
```

Set at least these values in the repo-root `.env`:

- `RFX_DATABASE_URL`
- `RFX_STORAGE_ROOT`
- `LLM_API_KEY`

The backend, Alembic, and CLI commands read the repo-root `.env` automatically. You do not need to `source` it for the documented commands.

Important database note for a first-time clone:

- the default `.env.example` points to `rfx_rag_expert`
- Alembic reads that value directly from the repo-root `.env`
- the target database must already exist before `alembic upgrade head`
- if you change `RFX_DATABASE_URL`, create that database in PostgreSQL before running Alembic

The frontend reads Vite `VITE_*` variables from `frontend/.env.local`. The default example config uses a neutral local tenant/user identity for local runs. Developer-only panels stay hidden unless you explicitly set `VITE_ENABLE_DEV_PANELS=true`.

### 4. Apply the public baseline migration

This creates the application schema inside the database named by `RFX_DATABASE_URL`.

```bash
python3 -m alembic -c backend/alembic.ini upgrade head
```

If Alembic fails with `database "..." does not exist`, create the database named in `RFX_DATABASE_URL` first and then rerun the migration.

### 5. Ensure the local identity

This creates the local tenant and local user expected by the default local frontend configuration.

```bash
make ensure-local-identity
```

This creates the local tenant and user used by the default local setup.

### 6. Load the sample corpus

This imports the synthetic historical exemplar corpus and canonical product-truth records used by the local sample setup.

Load historical exemplars:

```bash
make import-historical-corpus
```

Load product truth:

```bash
make import-product-truth
```

The included product-truth source lives at [seed_data/product_truth/product_truth.json](seed_data/product_truth/product_truth.json).

If you want to load real customer data instead of the tracked synthetic sample files, point the CLI at any corpus root that matches the same layout:

```bash
python3 -m app.cli import-historical-corpus --base-path <private-corpus-root>
python3 -m app.cli reimport-product-truth --path <private-corpus-root>/product_truth/product_truth.json
```

When replacing an existing product-truth corpus, use `reimport-product-truth` rather than additive `import-product-truth`.

### 7. Run the services

This starts the backend API, the frontend UI, and optionally the worker that processes queued bulk-fill jobs.

Backend:

```bash
make run-backend
```

Frontend, in a second terminal:

```bash
make run-frontend
```

Bulk-fill worker, in a third terminal if you want bulk-fill to process queued jobs:

```bash
make run-bulk-fill-worker
```

The backend listens on `http://127.0.0.1:8000` and the Vite frontend on `http://127.0.0.1:5173`.

## Included Sample Data

The repo ships synthetic sample seed data under `seed_data/`, split into `seed_data/product_truth/` for vendor product truth and one directory per sample customer package.

- `make import-historical-corpus` imports historical customer Q&A exemplars.
- `make import-product-truth` imports canonical vendor product-truth records.
- The canonical cross-platform equivalents are `python -m app.cli import-historical-corpus` and `python -m app.cli import-product-truth`.
- real customer corpora can be loaded from any private corpus root that matches the same layout, instead of replacing the tracked sample files in `seed_data/`
- when replacing private product truth, use `python -m app.cli reimport-product-truth --path <private-corpus-root>/product_truth/product_truth.json`

Both are needed for the full local sample setup because the app keeps historical phrasing/examples separate from vendor-truth claims.

See [docs/sample-data.md](docs/sample-data.md).

## Common Commands

Canonical cross-platform commands after the backend package is installed into your active environment:

- `python -m alembic -c backend/alembic.ini upgrade head`: apply the schema migration
- `python -m app.cli ensure-local-identity`: create the local tenant and user
- `python -m app.cli import-historical-corpus`: import the sample historical exemplar corpus
- `python -m app.cli import-product-truth`: import the sample product-truth corpus
- `python -m uvicorn app.main:app --reload --port 8000`: start the FastAPI app
- `npm --prefix frontend run dev`: start the Vite frontend
- `python -m app.cli run-bulk-fill-worker`: start the bulk-fill worker loop
- `python -m app.cli run-bulk-fill-worker --once`: process at most one queued bulk-fill request

Unix/macOS convenience aliases remain available through `make` for install, migrate, run, lint, typecheck, and test flows.

## Local And Development Notes

- The public baseline migration is intended for a fresh database only.
- The included frontend uses local identity headers from `frontend/.env.local`. That is suitable for local/sample use and is not a production auth system.
- Developer-only panels and the backend data browser remain available for development, but they are hidden by default in the normal app experience.
- `make` remains a Unix convenience layer. The Win11 onboarding surface is `scripts/windows/dev.ps1`.

## Advanced Docs

- [docs/system-architecture.md](docs/system-architecture.md)
- [docs/conceptual-code-model.md](docs/conceptual-code-model.md)
- [docs/rag-data-contract.md](docs/rag-data-contract.md)
- [docs/product-truth-contract.md](docs/product-truth-contract.md)
- [docs/pipeline-config.md](docs/pipeline-config.md)
- [docs/reproducibility-architecture.md](docs/reproducibility-architecture.md)
- [docs/docker-compose-setup.md](docs/docker-compose-setup.md)

## Boundary With External Evaluation

This repository is the application boundary. It exposes the app, CLI, and runtime configuration surface. It does **not** include sweep logic, benchmark orchestration, scoring, split management, or adapter code. Those belong in a separate evaluation repo that drives this app from the outside.
