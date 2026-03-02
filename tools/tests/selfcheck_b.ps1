$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $Root

function Ok($m){ Write-Host "[OK] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Fail($m){ Write-Host "[FAIL] $m" -ForegroundColor Red; exit 1 }

$py = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $py)) { Fail ".venv not found. Run 0_SETUP.bat first." }
Ok ".venv found"

$idea = Join-Path $Root "ideas\IDEA-20260302-002"
if (-not (Test-Path $idea)) {
  $idea = Get-ChildItem (Join-Path $Root "ideas") -Directory | Where-Object { $_.Name -like "IDEA-*" } | Sort-Object Name -Descending | Select-Object -First 1 | ForEach-Object { $_.FullName }
}
if (-not $idea) { Fail "No IDEA-* folder found" }
Ok "Idea selected: $idea"

& $py -m pip install -r (Join-Path $Root "tools\requirements_b.txt") | Out-Null

$module = Join-Path $Root "tools\module_b_lit_scout.py"
$code = 0
& $py $module --idea $idea --mode BALANCED
$code = $LASTEXITCODE

if ($code -ne 0) {
  Warn "Online run failed, switching to offline fixtures"
}

$corpusPath = Join-Path $idea "out\corpus.csv"
$linesOnline = 0
if (Test-Path $corpusPath) { $linesOnline = (Get-Content -LiteralPath $corpusPath | Measure-Object -Line).Lines }
if ($code -ne 0 -or $linesOnline -le 1) {
  Warn "Running offline fixtures validation"
  & $py $module --idea $idea --mode BALANCED --offline-fixtures (Join-Path $Root "tools\tests\fixtures")
  if ($LASTEXITCODE -ne 0) { Fail "Offline run failed too" }
}

$need = @(
  "out\corpus.csv",
  "out\corpus_all.csv",
  "out\stageB_summary.txt",
  "out\search_log_B.json",
  "out\prisma_lite_B.md",
  "out\runB.log"
)

foreach($rel in $need){
  $p = Join-Path $idea $rel
  if (-not (Test-Path $p)) { Fail "Missing output: $rel" }
  Ok "Exists: $rel"
}

$rows = (Get-Content -LiteralPath (Join-Path $idea "out\corpus.csv") | Measure-Object -Line).Lines
Ok "corpus.csv lines: $rows"
Write-Host "Selfcheck B complete"
