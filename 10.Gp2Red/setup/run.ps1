$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$pythonPath = Join-Path $projectRoot ".venv\\Scripts\\python.exe"

if (-not (Test-Path $pythonPath)) {
    throw "먼저 .\\setup\\install.ps1을 실행하십시오."
}

& $pythonPath -m streamlit run (Join-Path $projectRoot "app\\SrcTgtOrchestrator.py") --server.port 8502
