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
$script:LogTimestampFormat = "yyyy-MM-dd HH:mm:ss.fff zzz"
$script:InvocationStartedAt = Get-Date

function Get-LogTimestamp {
    return (Get-Date).ToString($script:LogTimestampFormat)
}

function Format-Duration {
    param([TimeSpan]$Duration)
    if ($Duration.TotalHours -ge 1) {
        return "{0:hh\:mm\:ss\.fff}" -f $Duration
    }
    return "{0:mm\:ss\.fff}" -f $Duration
}

function Write-Log {
    param(
        [string]$Level,
        [string]$Message
    )
    Write-Host "[$(Get-LogTimestamp)] [$Level] $Message"
}

function Write-Step {
    param(
        [string]$Message,
        [string]$MakeTarget = ""
    )
    if ($MakeTarget) {
        Write-Log -Level "STEP" -Message "$Message (Linux equivalent: make $MakeTarget)"
        return
    }
    Write-Log -Level "STEP" -Message $Message
}

function Format-CommandPart {
    param([AllowEmptyString()][string]$Value)
    if ($Value -eq "") {
        return "''"
    }
    if ($Value -match '[\s''"]') {
        return "'" + $Value.Replace("'", "''") + "'"
    }
    return $Value
}

function Format-CommandLine {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Executable,
        [string[]]$Arguments = @()
    )
    $parts = @($Executable) + $Arguments
    return (($parts | ForEach-Object { Format-CommandPart -Value $_ }) -join " ")
}

function Format-EnvironmentOverrides {
    param([hashtable]$Environment)
    if ($Environment.Count -eq 0) {
        return ""
    }
    return (
        $Environment.GetEnumerator() |
            Sort-Object Name |
            ForEach-Object { "$($_.Key)=$($_.Value)" }
    ) -join "; "
}

function Resolve-RepoPath {
    param([string]$RelativePath)
    return Join-Path $script:RepoRoot $RelativePath
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
        Write-Log -Level "INFO" -Message "template=$TemplatePath target=$TargetPath"
        return
    }
    Copy-Item -LiteralPath $TemplatePath -Destination $TargetPath -Force
    Write-Step "Wrote $(Split-Path -Leaf $TargetPath) from template"
    Write-Log -Level "INFO" -Message "template=$TemplatePath target=$TargetPath"
}

function Invoke-ExternalCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Executable,
        [string[]]$Arguments = @(),
        [Parameter(Mandatory = $true)]
        [string]$FailureMessage,
        [string]$WorkingDirectory = $script:RepoRoot,
        [hashtable]$Environment = @{},
        [string]$Description = ""
    )

    $label = if ($Description) { $Description } else { [System.IO.Path]::GetFileName($Executable) }
    $startedAt = Get-Date
    $environmentSummary = Format-EnvironmentOverrides -Environment $Environment
    $previousEnvironment = @{}

    Write-Log -Level "CMD" -Message "Starting $label"
    Write-Log -Level "CMD" -Message "cwd=$WorkingDirectory"
    Write-Log -Level "CMD" -Message "exec=$(Format-CommandLine -Executable $Executable -Arguments $Arguments)"
    if ($environmentSummary) {
        Write-Log -Level "CMD" -Message "env=$environmentSummary"
    }

    foreach ($entry in $Environment.GetEnumerator()) {
        $previousEnvironment[$entry.Key] = [Environment]::GetEnvironmentVariable($entry.Key, "Process")
        [Environment]::SetEnvironmentVariable($entry.Key, [string]$entry.Value, "Process")
    }

    Push-Location -LiteralPath $WorkingDirectory
    try {
        & $Executable @Arguments
        if ($LASTEXITCODE -ne 0) {
            throw "$FailureMessage Exit code=$LASTEXITCODE."
        }
        Write-Log -Level "DONE" -Message "$label completed in $(Format-Duration ((Get-Date) - $startedAt))"
    }
    catch {
        Write-Log -Level "FAIL" -Message "$label failed after $(Format-Duration ((Get-Date) - $startedAt))"
        throw
    }
    finally {
        Pop-Location
        foreach ($entry in $Environment.GetEnumerator()) {
            [Environment]::SetEnvironmentVariable($entry.Key, $previousEnvironment[$entry.Key], "Process")
        }
    }
}

function Invoke-PythonScript {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PythonExe,
        [Parameter(Mandatory = $true)]
        [string]$Code,
        [string[]]$Arguments = @(),
        [string]$Description = "Python helper"
    )
    $tempFile = Join-Path ([System.IO.Path]::GetTempPath()) ("rfx-win-" + [guid]::NewGuid().ToString() + ".py")
    Set-Content -LiteralPath $tempFile -Value $Code -Encoding UTF8
    Write-Log -Level "INFO" -Message "Wrote $Description to temporary helper $tempFile"
    try {
        Invoke-ExternalCommand `
            -Executable $PythonExe `
            -Arguments (@("-u", $tempFile) + $Arguments) `
            -FailureMessage "Python helper command failed." `
            -Environment @{ PYTHONUNBUFFERED = "1" } `
            -Description $Description
    }
    finally {
        Remove-Item -LiteralPath $tempFile -ErrorAction SilentlyContinue
        Write-Log -Level "INFO" -Message "Removed temporary helper $tempFile"
    }
}

function Invoke-RepoPython {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PythonExe,
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,
        [string]$Description = "Python command"
    )
    Invoke-ExternalCommand `
        -Executable $PythonExe `
        -Arguments (@("-u") + $Arguments) `
        -FailureMessage "Python command failed." `
        -Environment @{ PYTHONUNBUFFERED = "1" } `
        -Description $Description
}

function Invoke-RepoNpm {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,
        [string]$Description = "npm command"
    )
    $npmExe = Resolve-CommandPath -Names @("npm.cmd", "npm") -FailureMessage "npm was not found on PATH. Install Node.js 20+ first."
    Invoke-ExternalCommand `
        -Executable $npmExe `
        -Arguments $Arguments `
        -FailureMessage "npm command failed." `
        -Description $Description
}

function Invoke-Bootstrap {
    $venvPath = Resolve-RepoPath ".venv"
    $venvPython = Resolve-RepoPath ".venv\Scripts\python.exe"
    if (-not (Test-Path -LiteralPath $venvPython)) {
        $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
        if ($null -ne $pyLauncher) {
            Write-Step "Creating .venv with py -3.12"
            Invoke-ExternalCommand `
                -Executable $pyLauncher.Source `
                -Arguments @("-3.12", "-m", "venv", $venvPath) `
                -FailureMessage "py -3.12 -m venv failed." `
                -Description "Create .venv with py launcher"
        }
        else {
            $pythonExe = Resolve-CommandPath -Names @("python") -FailureMessage "Neither py nor python was found on PATH. Install Python 3.12+ first."
            Write-Step "Creating .venv with python -m venv"
            Invoke-ExternalCommand `
                -Executable $pythonExe `
                -Arguments @("-m", "venv", $venvPath) `
                -FailureMessage "python -m venv failed." `
                -Description "Create .venv with python"
        }
    }
    else {
        Write-Step "Reusing existing .venv"
        Write-Log -Level "INFO" -Message "venv=$venvPython"
    }

    $pythonExe = Resolve-VenvPython
    Write-Step "Upgrading pip"
    Invoke-RepoPython -PythonExe $pythonExe -Arguments @("-m", "pip", "install", "-U", "pip") -Description "Upgrade pip in repo virtualenv"

    Write-Step "Installing backend dependencies" -MakeTarget "install-backend"
    Invoke-RepoPython -PythonExe $pythonExe -Arguments @("-m", "pip", "install", "-e", ".\backend[dev]") -Description "Install backend dependencies"

    Write-Step "Installing frontend dependencies" -MakeTarget "install-frontend"
    Invoke-RepoNpm -Arguments @("--prefix", "frontend", "install") -Description "Install frontend dependencies"

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

    $initDbPython = @'
from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

import psycopg
from psycopg import sql
from sqlalchemy.engine import make_url

from app.config import build_settings


def psycopg_connect_url(raw_url: str) -> str:
    return raw_url.replace("postgresql+psycopg://", "postgresql://", 1)


def log(message: str) -> None:
    timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
    print(f"[{timestamp}] [init-db-helper] {message}", flush=True)


root = Path(sys.argv[1])
log(f"Loading settings from {root / '.env'}")
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
log(f"Target database URL={url.render_as_string(hide_password=True)}")
log(f"Maintenance database URL={maintenance_url.render_as_string(hide_password=True)}")
log(f"psycopg connect_timeout={url.query.get('connect_timeout', 'driver default')}")

try:
    log("Connecting to maintenance database")
    with psycopg.connect(maintenance_connect_url, autocommit=True) as connection:
        server_version = connection.execute("SHOW server_version").fetchone()[0]
        log(f"Connected to maintenance database; server_version={server_version}")
        log("Running PostgreSQL reachability probe: SELECT 1")
        connection.execute("SELECT 1")
        log(f"Checking whether database {url.database} exists")
        exists = connection.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (url.database,),
        ).fetchone() is not None
        if not exists:
            log(f"Creating database {url.database}")
            connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(url.database)))
            log(f"Created database {url.database}")
        else:
            log(f"Database {url.database} already exists")
except Exception as exc:  # pragma: no cover - exercised by PowerShell workflow/manual use
    raise SystemExit(
        "Could not connect to PostgreSQL using RFX_DATABASE_URL. "
        "Ensure the server is running and the credentials are correct."
    ) from exc

try:
    log(f"Connecting to target database {url.database}")
    with psycopg.connect(target_connect_url, autocommit=True) as connection:
        log("Checking pgvector availability in pg_available_extensions")
        vector_available = connection.execute(
            "SELECT 1 FROM pg_available_extensions WHERE name = 'vector'"
        ).fetchone() is not None
        if not vector_available:
            raise SystemExit(
                "The PostgreSQL server does not expose the 'vector' extension yet. "
                "Install pgvector first, then rerun init-db. See docs/windows-local-setup.md."
            )
        log("Verified pgvector availability")
except Exception as exc:  # pragma: no cover - exercised by PowerShell workflow/manual use
    if isinstance(exc, SystemExit):
        raise
    raise SystemExit(
        "Could not connect to the target database after creation. "
        "Ensure RFX_DATABASE_URL points to a reachable PostgreSQL database."
    ) from exc
'@
    Write-Step "Verifying PostgreSQL reachability, database existence, and pgvector availability"
    Invoke-PythonScript -PythonExe $pythonExe -Code $initDbPython -Arguments @($script:RepoRoot) -Description "init-db Python helper"

    Write-Step "Applying schema migration" -MakeTarget "migrate"
    Write-Log -Level "INFO" -Message "Alembic can be quiet while PostgreSQL executes DDL; statement-level progress will stream from the migration."
    Invoke-RepoPython -PythonExe $pythonExe -Arguments @("-m", "alembic", "-c", "backend/alembic.ini", "upgrade", "head") -Description "Alembic schema migration"

    Write-Step "Ensuring local tenant and user" -MakeTarget "ensure-local-identity"
    Invoke-RepoPython -PythonExe $pythonExe -Arguments @("-m", "app.cli", "ensure-local-identity") -Description "Ensure local tenant and user"

    Write-Step "Database initialization complete"
}

function Invoke-SeedSample {
    $pythonExe = Resolve-VenvPython
    Write-Step "Importing historical sample corpus" -MakeTarget "import-historical-corpus"
    Invoke-RepoPython -PythonExe $pythonExe -Arguments @("-m", "app.cli", "import-historical-corpus") -Description "Import historical sample corpus"
    Write-Step "Importing product truth sample corpus" -MakeTarget "import-product-truth"
    Invoke-RepoPython -PythonExe $pythonExe -Arguments @("-m", "app.cli", "import-product-truth") -Description "Import product truth sample corpus"
}

function Invoke-RunBackend {
    $pythonExe = Resolve-VenvPython
    Write-Step "Starting backend API server" -MakeTarget "run-backend"
    Invoke-RepoPython -PythonExe $pythonExe -Arguments @("-m", "uvicorn", "app.main:app", "--reload", "--port", "8000") -Description "Run backend API server"
}

function Invoke-RunFrontend {
    Write-Step "Starting frontend dev server" -MakeTarget "run-frontend"
    Invoke-RepoNpm -Arguments @("--prefix", "frontend", "run", "dev") -Description "Run frontend dev server"
}

function Invoke-RunWorker {
    $pythonExe = Resolve-VenvPython
    Write-Step "Starting bulk-fill worker" -MakeTarget "run-bulk-fill-worker"
    Invoke-RepoPython -PythonExe $pythonExe -Arguments @("-m", "app.cli", "run-bulk-fill-worker") -Description "Run bulk-fill worker"
}

Write-Log -Level "INFO" -Message "Starting scripts/windows/dev.ps1 command=$Command repo_root=$script:RepoRoot force_env=$ForceEnv"

try {
    switch ($Command) {
        "bootstrap" { Invoke-Bootstrap }
        "init-db" { Invoke-InitDb }
        "seed-sample" { Invoke-SeedSample }
        "run-backend" { Invoke-RunBackend }
        "run-frontend" { Invoke-RunFrontend }
        "run-worker" { Invoke-RunWorker }
        default { throw "Unsupported command: $Command" }
    }
    Write-Log -Level "DONE" -Message "Command $Command completed in $(Format-Duration ((Get-Date) - $script:InvocationStartedAt))"
}
catch {
    Write-Log -Level "FAIL" -Message "Command $Command failed after $(Format-Duration ((Get-Date) - $script:InvocationStartedAt))"
    throw
}
