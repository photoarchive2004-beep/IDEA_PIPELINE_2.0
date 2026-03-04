param(
  [ValidateSet("balanced","wide","focused")]
  [string]$Scope = "balanced",
  [int]$N = 300,
  [string]$IdeaDir = ""
)

$mode = $Scope.ToUpperInvariant()
& "$PSScriptRoot\run_b.ps1" -Mode $mode -N $N -IdeaDir $IdeaDir
exit $LASTEXITCODE
