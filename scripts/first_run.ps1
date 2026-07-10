Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Require-Command {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command not found: $Name"
    }
}

Write-Step "Checking local prerequisites"
Require-Command python
Require-Command node
Require-Command npm
Require-Command java

if (-not (Test-Path (Join-Path $RepoRoot ".venv"))) {
    Write-Step "Creating Python virtual environment"
    & python -m venv (Join-Path $RepoRoot ".venv")
}

Write-Step "Installing Python dependencies"
& $VenvPython -m pip install --upgrade pip
& $VenvPython -m pip install -r (Join-Path $RepoRoot "requirements.txt")

Write-Step "Installing dashboard dependencies"
Push-Location (Join-Path $RepoRoot "dashboard")
try {
    & npm install
}
finally {
    Pop-Location
}

Write-Step "Preparing local runtime directories"
$paths = @(
    "data\streaming_output\demand_agg",
    "data\streaming_output\hostel_agg",
    "tmp\checkpoints",
    "tmp\spark-local"
)
foreach ($relativePath in $paths) {
    $absolutePath = Join-Path $RepoRoot $relativePath
    if (-not (Test-Path $absolutePath)) {
        New-Item -ItemType Directory -Path $absolutePath | Out-Null
    }
}

Write-Step "Generating seed training data"
& $VenvPython (Join-Path $RepoRoot "data\seed\generate_seed_data.py")

Write-Step "Bootstrap complete"
Write-Host "Next steps:" -ForegroundColor Green
Write-Host "  1. Ensure JAVA_HOME, HADOOP_HOME, KAFKA_HOME, and optionally SPARK_HOME are set."
Write-Host "  2. Run .\scripts\start_stack.ps1 to launch the local stack."
Write-Host "  3. Open http://localhost:3000 after the dashboard starts."
