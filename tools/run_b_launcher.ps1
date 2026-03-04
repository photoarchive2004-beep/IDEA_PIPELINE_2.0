param(
  [ValidateSet("balanced","wide","focused")]
  [string]$Scope = "balanced",
  [int]$N = 300,
  [string]$IdeaDir = "",
  [switch]$CleanHard
)

[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
$OutputEncoding = [Text.UTF8Encoding]::new($false)
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"

$mode = $Scope.ToUpperInvariant()
& "$PSScriptRoot\run_b.ps1" -Mode $mode -N $N -IdeaDir $IdeaDir -CleanHard:$CleanHard
exit $LASTEXITCODE
