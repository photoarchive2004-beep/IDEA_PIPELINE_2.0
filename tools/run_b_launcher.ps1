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

$root = Split-Path -Parent $PSScriptRoot
$logDir = Join-Path $root "launcher_logs"
$lastLog = Join-Path $logDir "runB_last.log"
$launcherErrLog = Join-Path $logDir "runB_launcher_error.log"

try {
  New-Item -ItemType Directory -Force -Path $logDir | Out-Null
  "START run_b_launcher $(Get-Date -Format o) Scope=$Scope N=$N IdeaDir=$IdeaDir CleanHard=$($CleanHard.IsPresent)" | Out-File -FilePath $lastLog -Encoding UTF8

  $mode = $Scope.ToUpperInvariant()
  & "$PSScriptRoot\run_b.ps1" -Mode $mode -N $N -IdeaDir $IdeaDir -CleanHard:$CleanHard
  $rc = $LASTEXITCODE

  if ($rc -ne 0) {
    "run_b.ps1 failed with code $rc" | Out-File -FilePath $lastLog -Append -Encoding UTF8
    exit $rc
  }

  "OK run_b_launcher" | Out-File -FilePath $lastLog -Append -Encoding UTF8
  exit 0
}
catch {
  New-Item -ItemType Directory -Force -Path $logDir | Out-Null
  $details = @(
    "ERROR run_b_launcher $(Get-Date -Format o)",
    "Message: $($_.Exception.Message)",
    "Type: $($_.Exception.GetType().FullName)",
    "Stack:",
    ($_.ScriptStackTrace),
    "Full:",
    ($_ | Out-String)
  ) -join [Environment]::NewLine

  $details | Out-File -FilePath $launcherErrLog -Encoding UTF8
  $details | Out-File -FilePath $lastLog -Encoding UTF8
  exit 1
}
