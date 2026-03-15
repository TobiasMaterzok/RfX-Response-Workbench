# Docker Compose setup

This repo now supports a containerized local run path for Win11, macOS, and Linux.
It is the recommended path when host `npm` usage is restricted by corporate policy, because Node and npm stay inside the frontend container.

The Compose stack uses Linux containers for:

- `postgres` with `pgvector`
- `backend` for the FastAPI API
- `frontend` for the Vite UI
- `worker` for queued bulk-fill jobs

Your browser stays on the host machine. On Win11, open the UI from Docker Desktop or any browser at `http://127.0.0.1:5173`.

## Prerequisites

- Docker Desktop or another recent Docker Engine with `docker compose`
- an `LLM_API_KEY` if you want extraction, drafting, embeddings, or seed imports to work

## 1. Prepare environment

Copy the standard repo env file if you do not already have one:

```bash
cp .env.example .env
```

Set at least:

- `LLM_API_KEY`

Optional values reused by the containers:

- `LLM_API_BASE_URL`
- `RFX_OPENAI_RESPONSE_MODEL`
- `RFX_OPENAI_EMBEDDING_MODEL`
- `RFX_LOCAL_TENANT_SLUG`
- `RFX_LOCAL_TENANT_NAME`
- `RFX_LOCAL_USER_EMAIL`
- `RFX_LOCAL_USER_NAME`
- `VITE_API_BASE_URL`
- `VITE_TENANT_SLUG`
- `VITE_USER_EMAIL`
- `VITE_ENABLE_DEV_PANELS`

Compose pins container-safe defaults for the database and storage paths, so the host-oriented `RFX_DATABASE_URL` and `RFX_STORAGE_ROOT` values in `.env` are ignored by this stack.
Legacy `OPENAI_API_KEY` and `OPENAI_BASE_URL` aliases are still accepted. For Azure OpenAI, set `LLM_API_BASE_URL=https://YOUR_RESOURCE_NAME.openai.azure.com/openai/v1/` and use your Azure deployment names in `RFX_OPENAI_RESPONSE_MODEL` and `RFX_OPENAI_EMBEDDING_MODEL`.

## 2. Start the app

Bring up PostgreSQL, run the one-shot init service, then start the backend and frontend:

```bash
docker pull pgvector/pgvector:pg18-trixie
docker compose up --build -d postgres backend frontend
```

If you previously started the stack with an older PostgreSQL image layout or an older version of this repo that mounted the database volume at `/var/lib/postgresql/data`, the first boot on `pg18-trixie` can fail with a message about `pg_ctlcluster` and an unused mount at `/var/lib/postgresql/data`.

For this repo's disposable local stack, the clean fix is:

```bash
docker compose down -v
docker compose up --build -d postgres backend frontend
```

Only do `docker compose down -v` if you are willing to delete the local database volume for this stack.

What happens:

- `postgres` starts on `127.0.0.1:5432`
- `init` runs automatically once, applies Alembic, and ensures the local tenant/user
- `backend` starts on `127.0.0.1:8000`
- `frontend` starts on `127.0.0.1:5173`

Open the UI at:

```text
http://127.0.0.1:5173
```

## 3. Start the bulk-fill worker

Run the worker when you want queued bulk-fill jobs to execute:

```bash
docker compose up -d worker
```

## 4. Load data

The containers mount `./seed_data` read-only at `/app/seed_data`, so both the tracked synthetic sample data and any private corpus you place under `seed_data/local/` are visible inside the backend container.

Sample/demo import:

```bash
docker compose exec backend python -m app.cli import-historical-corpus
docker compose exec backend python -m app.cli import-product-truth
```

Do not mix this with `powershell -ExecutionPolicy Bypass -File .\scripts\windows\dev.ps1 seed-sample` while the full Compose app stack is your active runtime. The seed import must run in the same storage/runtime context as the backend and worker, which for this path is the backend container plus the `rfx-storage` volume.

Private/local corpus import:

```bash
docker compose exec backend python -m app.cli import-historical-corpus --base-path /app/seed_data/local
docker compose exec backend python -m app.cli reimport-product-truth --path /app/seed_data/local/product_truth/product_truth.json
```

## 5. Logs and shutdown

Tail logs:

```bash
docker compose logs -f backend frontend worker
```

Stop the stack but keep data:

```bash
docker compose down
```

Reset everything, including the PostgreSQL data and shared object-storage volume:

```bash
docker compose down -v
```

`docker compose down -v` is destructive. It removes the local database and stored uploads/exports for this Compose stack.
