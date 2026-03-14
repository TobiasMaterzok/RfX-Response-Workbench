# Windows 11 local setup

This repo supports a native Win11 local run path for the application itself.
It still expects **PostgreSQL 16 + `pgvector`** as prerequisites for the local database.

The repo-owned automation starts after those prerequisites exist:

- it bootstraps the Python and frontend dependencies
- it validates PostgreSQL reachability
- it creates the target database if missing
- it verifies that the `vector` extension is available
- it applies the schema migration
- it bootstraps the local tenant and user

## Prerequisites

Install these first:

- Python 3.12+
- Node.js 20+
- PostgreSQL 16 with command-line tools (`psql.exe`) on `PATH`
- `pgvector` installed for that PostgreSQL instance so `vector` appears in `pg_available_extensions`

Official prerequisite docs:

- PostgreSQL Windows installers: <https://www.postgresql.org/download/windows/>
- pgvector Windows install notes: <https://github.com/pgvector/pgvector?tab=readme-ov-file#windows>

The helper script in this repo does **not** install PostgreSQL, Visual Studio C++ tooling, or pgvector for you.

## First-time local run

Open PowerShell in the repo root and run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\windows\dev.ps1 bootstrap
```

`bootstrap` creates `.env` and `frontend\.env.local` if they do not already exist.

Edit the repo-root `.env` and set at least:

- `RFX_DATABASE_URL`
- `RFX_STORAGE_ROOT`
- `OPENAI_API_KEY`

Then initialize the local database and identity:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\windows\dev.ps1 init-db
```

Sample/demo path:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\windows\dev.ps1 seed-sample
```

Real-customer/private corpus path:

Historical data can be loaded from any corpus root that matches the expected manifest and folder layout. One local private example is `seed_data\local\`. The supported Win11 commands are:

```powershell
.\.venv\Scripts\python.exe -m app.cli import-historical-corpus --base-path <private-corpus-root>
.\.venv\Scripts\python.exe -m app.cli reimport-product-truth --path <private-corpus-root>\product_truth\product_truth.json
```

When replacing an existing product-truth corpus, use `reimport-product-truth` rather than additive `import-product-truth`.

Run the services in separate PowerShell windows:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\windows\dev.ps1 run-backend
powershell -ExecutionPolicy Bypass -File .\scripts\windows\dev.ps1 run-frontend
powershell -ExecutionPolicy Bypass -File .\scripts\windows\dev.ps1 run-worker
```

## Troubleshooting

### `psql.exe` is missing

Install the PostgreSQL client tools and ensure `psql.exe` is on `PATH`.

### `vector` is unavailable

The helper checks `pg_available_extensions` before running Alembic.
If the helper reports that `vector` is unavailable, finish the pgvector installation for your PostgreSQL instance first, then rerun `init-db`.

### PowerShell execution policy blocks the helper

Run the helper through:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\windows\dev.ps1 bootstrap
```

### Corporate SSL inspection breaks `pip install`

Retry the backend install inside the activated venv with trusted hosts:

```powershell
.\.venv\Scripts\python.exe -m pip install -U pip --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org
.\.venv\Scripts\python.exe -m pip install -e ".\backend[dev]" --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org
```

### Repo path contains spaces

The Win11 helper resolves repo-relative paths from its own script location, so cloning into a path with spaces is supported.
