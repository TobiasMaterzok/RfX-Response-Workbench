[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("bootstrap", "init-db", "seed-sample", "run-backend", "run-frontend", "run-worker")]
    [string]$Command,
    [switch]$ForceEnv
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$script:ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$script:RepoRoot = (Resolve-Path (Join-Path $script:ScriptRoot "..\..")).Path
$script:WindowsSetupDoc = "docs/windows-local-setup.md"
$script:BootstrapGuidance = 'Run `powershell -ExecutionPolicy Bypass -File .\scripts\windows\dev.ps1 bootstrap` first.'

function Write-Step {
    param([string]$Message)
    Write-Host "==> $Message"
}

function Resolve-RepoPath {
    param([string]$RelativePath)
    return Join-Path $script:RepoRoot $RelativePath
}

function Invoke-InRepo {
    param(
        [Parameter(Mandatory = $true)]
        [scriptblock]$ScriptBlock
    )
    Push-Location -LiteralPath $script:RepoRoot
    try {
        & $ScriptBlock
    }
    finally {
        Pop-Location
    }
}

function Resolve-CommandPath {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Names,
        [Parameter(Mandatory = $true)]
        [string]$FailureMessage
    )
    foreach ($name in $Names) {
        $commandInfo = Get-Command $name -ErrorAction SilentlyContinue
        if ($null -ne $commandInfo) {
            return $commandInfo.Source
        }
    }
    throw $FailureMessage
}

function Resolve-VenvPython {
    $pythonExe = Resolve-RepoPath ".venv\Scripts\python.exe"
    if (-not (Test-Path -LiteralPath $pythonExe)) {
        throw "The backend virtual environment is missing at .venv. $script:BootstrapGuidance"
    }
    return $pythonExe
}

function Copy-TemplateIfNeeded {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TemplatePath,
        [Parameter(Mandatory = $true)]
        [string]$TargetPath
    )
    if ((Test-Path -LiteralPath $TargetPath) -and -not $ForceEnv) {
        Write-Step "Preserving existing $(Split-Path -Leaf $TargetPath)"
        return
    }
    Copy-Item -LiteralPath $TemplatePath -Destination $TargetPath -Force
    Write-Step "Wrote $(Split-Path -Leaf $TargetPath) from template"
}

function Invoke-PythonScript {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PythonExe,
        [Parameter(Mandatory = $true)]
        [string]$Code,
        [string[]]$Arguments = @()
    )
    $tempFile = Join-Path ([System.IO.Path]::GetTempPath()) ("rfx-win-" + [guid]::NewGuid().ToString() + ".py")
    Set-Content -LiteralPath $tempFile -Value $Code -Encoding UTF8
    try {
        & $PythonExe $tempFile @Arguments
        if ($LASTEXITCODE -ne 0) {
            throw "Python helper command failed with exit code $LASTEXITCODE."
        }
    }
    finally {
        Remove-Item -LiteralPath $tempFile -ErrorAction SilentlyContinue
    }
}

function Invoke-RepoPython {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PythonExe,
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )
    Invoke-InRepo {
        & $PythonExe @Arguments
        if ($LASTEXITCODE -ne 0) {
            throw "Python command failed with exit code $LASTEXITCODE."
        }
    }
}

function Invoke-RepoNpm {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )
    $npmExe = Resolve-CommandPath -Names @("npm.cmd", "npm") -FailureMessage "npm was not found on PATH. Install Node.js 20+ first."
    Invoke-InRepo {
        & $npmExe @Arguments
        if ($LASTEXITCODE -ne 0) {
            throw "npm command failed with exit code $LASTEXITCODE."
        }
    }
}

function Invoke-Bootstrap {
    $venvPath = Resolve-RepoPath ".venv"
    $venvPython = Resolve-RepoPath ".venv\Scripts\python.exe"
    if (-not (Test-Path -LiteralPath $venvPython)) {
        $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
        if ($null -ne $pyLauncher) {
            Write-Step "Creating .venv with py -3.12"
            Invoke-InRepo {
                & $pyLauncher.Source -3.12 -m venv $venvPath
                if ($LASTEXITCODE -ne 0) {
                    throw "py -3.12 -m venv failed."
                }
            }
        }
        else {
            $pythonExe = Resolve-CommandPath -Names @("python") -FailureMessage "Neither py nor python was found on PATH. Install Python 3.12+ first."
            Write-Step "Creating .venv with python -m venv"
            Invoke-InRepo {
                & $pythonExe -m venv $venvPath
                if ($LASTEXITCODE -ne 0) {
                    throw "python -m venv failed."
                }
            }
        }
    }
    else {
        Write-Step "Reusing existing .venv"
    }

    $pythonExe = Resolve-VenvPython
    Write-Step "Upgrading pip"
    Invoke-RepoPython -PythonExe $pythonExe -Arguments @("-m", "pip", "install", "-U", "pip")

    Write-Step "Installing backend dependencies"
    Invoke-RepoPython -PythonExe $pythonExe -Arguments @("-m", "pip", "install", "-e", ".\backend[dev]")

    Write-Step "Installing frontend dependencies"
    Invoke-RepoNpm -Arguments @("--prefix", "frontend", "install")

    Copy-TemplateIfNeeded -TemplatePath (Resolve-RepoPath ".env.example") -TargetPath (Resolve-RepoPath ".env")
    Copy-TemplateIfNeeded -TemplatePath (Resolve-RepoPath "frontend\.env.example") -TargetPath (Resolve-RepoPath "frontend\.env.local")

    Write-Step "Bootstrap complete"
}

function Invoke-InitDb {
    $pythonExe = Resolve-VenvPython
    $envPath = Resolve-RepoPath ".env"
    if (-not (Test-Path -LiteralPath $envPath)) {
        throw "Missing .env at repo root. $script:BootstrapGuidance"
    }

    $psqlExe = Resolve-CommandPath -Names @("psql.exe", "psql") -FailureMessage "psql.exe was not found on PATH. Install PostgreSQL 16 client tools first. See $script:WindowsSetupDoc."
    Write-Step "Found psql at $psqlExe"

    $initDbPython = @'
from __future__ import annotations

from pathlib import Path
import sys

import psycopg
from psycopg import sql
from sqlalchemy.engine import make_url

from app.config import build_settings


def psycopg_connect_url(raw_url: str) -> str:
    return raw_url.replace("postgresql+psycopg://", "postgresql://", 1)


root = Path(sys.argv[1])
settings = build_settings(env_file=root / ".env")
url = make_url(settings.database_url)
if url.get_backend_name() != "postgresql":
    raise SystemExit(
        "RFX_DATABASE_URL must point to PostgreSQL for the Win11 local path. "
        "See docs/windows-local-setup.md."
    )
if not url.database:
    raise SystemExit("RFX_DATABASE_URL must include a database name.")

maintenance_url = url.set(database="postgres")
maintenance_connect_url = psycopg_connect_url(maintenance_url.render_as_string(hide_password=False))
target_connect_url = psycopg_connect_url(url.render_as_string(hide_password=False))

try:
    with psycopg.connect(maintenance_connect_url, autocommit=True) as connection:
        connection.execute("SELECT 1")
        exists = connection.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (url.database,),
        ).fetchone() is not None
        if not exists:
            connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(url.database)))
            print(f"Created database {url.database}")
        else:
            print(f"Database {url.database} already exists")
except Exception as exc:  # pragma: no cover - exercised by PowerShell workflow/manual use
    raise SystemExit(
        "Could not connect to PostgreSQL using RFX_DATABASE_URL. "
        "Ensure the server is running and the credentials are correct."
    ) from exc

try:
    with psycopg.connect(target_connect_url, autocommit=True) as connection:
        vector_available = connection.execute(
            "SELECT 1 FROM pg_available_extensions WHERE name = 'vector'"
        ).fetchone() is not None
        if not vector_available:
            raise SystemExit(
                "The PostgreSQL server does not expose the 'vector' extension yet. "
                "Install pgvector first, then rerun init-db. See docs/windows-local-setup.md."
            )
        print("Verified pgvector availability")
except Exception as exc:  # pragma: no cover - exercised by PowerShell workflow/manual use
    if isinstance(exc, SystemExit):
        raise
    raise SystemExit(
        "Could not connect to the target database after creation. "
        "Ensure RFX_DATABASE_URL points to a reachable PostgreSQL database."
    ) from exc
'@
    Write-Step "Verifying PostgreSQL reachability, database existence, and pgvector availability"
    Invoke-PythonScript -PythonExe $pythonExe -Code $initDbPython -Arguments @($script:RepoRoot)

    Write-Step "Applying schema migration"
    Invoke-RepoPython -PythonExe $pythonExe -Arguments @("-m", "alembic", "-c", "backend/alembic.ini", "upgrade", "head")

    Write-Step "Ensuring local tenant and user"
    Invoke-RepoPython -PythonExe $pythonExe -Arguments @("-m", "app.cli", "ensure-local-identity")

    Write-Step "Database initialization complete"
}

function Invoke-SeedSample {
    $pythonExe = Resolve-VenvPython
    Write-Step "Importing historical sample corpus"
    Invoke-RepoPython -PythonExe $pythonExe -Arguments @("-m", "app.cli", "import-historical-corpus")
    Write-Step "Importing product truth sample corpus"
    Invoke-RepoPython -PythonExe $pythonExe -Arguments @("-m", "app.cli", "import-product-truth")
}

function Invoke-RunBackend {
    $pythonExe = Resolve-VenvPython
    Invoke-RepoPython -PythonExe $pythonExe -Arguments @("-m", "uvicorn", "app.main:app", "--reload", "--port", "8000")
}

function Invoke-RunFrontend {
    Invoke-RepoNpm -Arguments @("--prefix", "frontend", "run", "dev")
}

function Invoke-RunWorker {
    $pythonExe = Resolve-VenvPython
    Invoke-RepoPython -PythonExe $pythonExe -Arguments @("-m", "app.cli", "run-bulk-fill-worker")
}

switch ($Command) {
    "bootstrap" { Invoke-Bootstrap }
    "init-db" { Invoke-InitDb }
    "seed-sample" { Invoke-SeedSample }
    "run-backend" { Invoke-RunBackend }
    "run-frontend" { Invoke-RunFrontend }
    "run-worker" { Invoke-RunWorker }
    default { throw "Unsupported command: $Command" }
}
