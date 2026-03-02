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
$fixture = Join-Path $Root "tools\tests\fixtures"

& $py $module --idea $idea --mode BALANCED
$rc = $LASTEXITCODE
if ($rc -ne 0 -and $rc -ne 2) {
  Warn "Online run failed (code $rc); using fixtures"
  & $py $module --idea $idea --mode BALANCED --offline-fixtures $fixture
  if ($LASTEXITCODE -ne 0 -and $LASTEXITCODE -ne 2) { Fail "Offline run failed" }
}

if ($rc -eq 2) {
  Warn "Stage B requested LLM cleaner in online mode; validating offline pipeline"
  & $py $module --idea $idea --mode BALANCED --offline-fixtures $fixture
  if ($LASTEXITCODE -ne 0 -and $LASTEXITCODE -ne 2) { Fail "Offline fallback failed" }
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

$corpusPath = Join-Path $idea "out\corpus.csv"
$header = (Get-Content -LiteralPath $corpusPath -TotalCount 1)
if ($header -notmatch "rank" -or $header -notmatch "score") { Fail "corpus.csv missing rank/score columns" }
Ok "corpus.csv has rank and score columns"

Write-Host "Selfcheck B complete"
