param(
  [ValidateSet("balanced","wide","focused")]
  [string]$Scope = "balanced",
  [int]$N = 300,
  [string]$IdeaDir = "",
  [switch]$CleanHard
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
$OutputEncoding = [Text.UTF8Encoding]::new($false)
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
if (-not $env:STAGE_B1_LLM_LIMIT -or [string]::IsNullOrWhiteSpace($env:STAGE_B1_LLM_LIMIT)) {
  $env:STAGE_B1_LLM_LIMIT = "10"
}

try {
  $mode = $Scope.ToUpperInvariant()
  & "$PSScriptRoot\run_b.ps1" -Mode $mode -N $N -IdeaDir $IdeaDir -CleanHard:$CleanHard
  exit $LASTEXITCODE
}
catch {
  $root = Split-Path -Parent $PSScriptRoot
  $logDir = Join-Path $root "launcher_logs"
  New-Item -ItemType Directory -Force -Path $logDir | Out-Null
  $log = Join-Path $logDir "runB_launcher_error.log"
  ($_ | Out-String) | Out-File -FilePath $log -Append -Encoding UTF8
  Write-Host "Launcher error. Log: $log"
  try { Start-Process notepad.exe -ArgumentList $log | Out-Null } catch {}
  exit 1
}