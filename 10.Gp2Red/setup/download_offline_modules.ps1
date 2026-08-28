param(
    [Parameter(Mandatory = $true)]
    [string]$Destination
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$pythonLauncher = Get-Command py -ErrorAction SilentlyContinue

if ($null -eq $pythonLauncher) {
    throw "Python 3.12 이상을 설치한 인터넷 연결 PC에서 실행하십시오."
}

New-Item -ItemType Directory -Path $Destination -Force | Out-Null
& py -3 -m pip download --dest $Destination -r (Join-Path $projectRoot "requirements.txt")
Write-Host "오프라인 모듈 폴더를 만들었습니다: $Destination"
