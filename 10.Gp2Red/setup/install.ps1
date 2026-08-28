param(
    [switch]$SkipPythonInstall
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$pythonLauncher = Get-Command py -ErrorAction SilentlyContinue

if ($null -eq $pythonLauncher) {
    if ($SkipPythonInstall) {
        throw "Python 3.12 이상을 설치한 뒤 setup\\install.ps1을 다시 실행하십시오."
    }
    $winget = Get-Command winget -ErrorAction SilentlyContinue
    if ($null -eq $winget) {
        throw "Python 자동 설치를 위해 winget이 필요합니다. Python 3.12 이상 설치 후 setup\\install.ps1을 다시 실행하십시오."
    }
    & winget install --id Python.Python.3.12 --exact --source winget --accept-package-agreements --accept-source-agreements
    $pythonLauncher = Get-Command py -ErrorAction SilentlyContinue
    if ($null -eq $pythonLauncher) {
        throw "Python 설치 후 PowerShell을 새로 열고 setup\\install.ps1을 다시 실행하십시오."
    }
}

$venvPath = Join-Path $projectRoot ".venv"
$pythonPath = Join-Path $venvPath "Scripts\\python.exe"
if (-not (Test-Path $pythonPath)) {
    & py -3 -m venv $venvPath
}

& $pythonPath -m pip install --upgrade pip
& $pythonPath -m pip install -r (Join-Path $projectRoot "requirements.txt")

$streamlitPath = Join-Path $projectRoot "app\\.streamlit"
$secretsPath = Join-Path $streamlitPath "secrets.toml"
if (-not (Test-Path $secretsPath)) {
    New-Item -ItemType Directory -Path $streamlitPath -Force | Out-Null
    Copy-Item -Path (Join-Path $projectRoot "setup\\secrets.toml") -Destination $secretsPath
}

Write-Host "설치가 완료되었습니다."
Write-Host "접속정보 입력 파일: $secretsPath"
Write-Host "실행 명령: .\\setup\\run.ps1"
