param(
    [switch]$SkipPythonInstall,
    [string]$WheelFolder
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$pythonLauncher = Get-Command py -ErrorAction SilentlyContinue

if ($null -eq $pythonLauncher) {
    if (-not [string]::IsNullOrWhiteSpace($WheelFolder)) {
        throw "오프라인 whl 설치에는 Python 3.12 이상이 먼저 설치되어 있어야 합니다."
    }
    if ($SkipPythonInstall) {
        throw "Python 3.12 이상을 설치한 뒤 setup\\install.ps1을 다시 실행하십시오."
    }
    $winget = Get-Command winget -ErrorAction SilentlyContinue
    if ($null -eq $winget) {
        throw "Python 자동 설치를 위해 winget이 필요합니다. Python 3.12 이상 설치 후 setup\\install.ps1을 다시 실행하십시오."
    }
    & winget install --id Python.Python.3.12 --exact --source winget --accept-package-agreements --accept-source-agreements
    $machinePath = [Environment]::GetEnvironmentVariable("Path", "Machine")
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    $env:Path = "$machinePath;$userPath"
    $pythonLauncher = Get-Command py -ErrorAction SilentlyContinue
    if ($null -eq $pythonLauncher) {
        throw "Python 설치 후 MigSetup.exe를 다시 실행하십시오."
    }
}

$venvPath = Join-Path $projectRoot ".venv"
$pythonPath = Join-Path $venvPath "Scripts\\python.exe"
if (-not (Test-Path $pythonPath)) {
    & py -3 -m venv $venvPath
}

$requirementsPath = Join-Path $projectRoot "requirements.txt"
if ([string]::IsNullOrWhiteSpace($WheelFolder)) {
    & $pythonPath -m pip install --upgrade pip
    & $pythonPath -m pip install -r $requirementsPath
}
else {
    if (-not (Test-Path $WheelFolder -PathType Container)) {
        throw "선택한 오프라인 모듈 폴더를 찾을 수 없습니다: $WheelFolder"
    }
    & $pythonPath -m pip install --no-index --find-links $WheelFolder -r $requirementsPath
}

$streamlitPath = Join-Path $projectRoot "app\\.streamlit"
$secretsPath = Join-Path $streamlitPath "secrets.toml"
if (-not (Test-Path $secretsPath)) {
    New-Item -ItemType Directory -Path $streamlitPath -Force | Out-Null
    Copy-Item -Path (Join-Path $projectRoot "setup\\secrets.toml") -Destination $secretsPath
}

Write-Host "설치가 완료되었습니다."
Write-Host "접속정보 입력 파일: $secretsPath"
Write-Host "접속정보 입력 후 setup\\run.ps1을 실행하십시오."
