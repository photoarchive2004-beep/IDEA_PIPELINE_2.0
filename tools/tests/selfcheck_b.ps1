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
& $py $module --idea $idea --mode BALANCED
$rc = $LASTEXITCODE

$corpusPath = Join-Path $idea "out\corpus.csv"
$waitPrompt = Join-Path $idea "out\llm_prompt_B_keywords.txt"
$waitResp = Join-Path $idea "in\llm_response_B.json"

if ($rc -eq 2) {
  if (-not (Test-Path $waitPrompt)) { Fail "WAIT mode without llm_prompt_B_keywords.txt" }
  if (-not (Test-Path $waitResp)) { Fail "WAIT mode without in\llm_response_B.json" }
  Ok "WAIT acceptance passed (exit code 2 + prompt/response files created)"
}
elseif ($rc -eq 0) {
  if (-not (Test-Path $corpusPath)) { Fail "Success mode but corpus.csv missing" }
  $rows = (Get-Content -LiteralPath $corpusPath | Measure-Object -Line).Lines
  if ($rows -le 10) {
    Warn "Online run returned <=10 rows; validating offline fixtures"
    & $py $module --idea $idea --mode BALANCED --offline-fixtures (Join-Path $Root "tools\tests\fixtures")
    if ($LASTEXITCODE -ne 0) { Fail "Offline fixture run failed" }
    $rows = (Get-Content -LiteralPath $corpusPath | Measure-Object -Line).Lines
    if ($rows -le 10) { Fail "corpus.csv still <=10 rows after offline fixture run" }
  }
  Ok "Success acceptance passed (corpus.csv >10 lines)"
}
else {
  Warn "Online run failed with code $rc; running offline fixtures"
  & $py $module --idea $idea --mode BALANCED --offline-fixtures (Join-Path $Root "tools\tests\fixtures")
  if ($LASTEXITCODE -ne 0) { Fail "Offline fixture run failed" }
  if (-not (Test-Path $corpusPath)) { Fail "Offline run produced no corpus.csv" }
  $rows = (Get-Content -LiteralPath $corpusPath | Measure-Object -Line).Lines
  if ($rows -le 10) { Fail "Offline run corpus.csv <=10 lines" }
  Ok "Offline acceptance passed"
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

Write-Host "Selfcheck B complete"
