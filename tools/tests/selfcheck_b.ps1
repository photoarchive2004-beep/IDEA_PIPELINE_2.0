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

$need = @("out\corpus.csv","out\corpus_all.csv","out\stageB_summary.txt","out\search_log_B.json","out\prisma_lite_B.md","out\runB.log")
foreach($rel in $need){
  $p = Join-Path $idea $rel
  if (-not (Test-Path $p)) { Fail "Missing output: $rel" }
  Ok "Exists: $rel"
}

$logPath = Join-Path $idea "out\search_log_B.json"
$searchLog = Get-Content -LiteralPath $logPath -Raw | ConvertFrom-Json
if (-not $searchLog.queries -or $searchLog.queries.Count -lt 1) { Fail "search_log_B.json has no queries" }
$first = $searchLog.queries[0]
if (-not ($first.PSObject.Properties.Name -contains "query_text") -or -not ($first.PSObject.Properties.Name -contains "result_total")) {
  Fail "search_log_B.json query entries missing query_text/result_total"
}
Ok "search_log_B.json has query_text and result_total"

# Deterministic stop-check: no valid latin anchors => rc 2 + llm files
$tempIdea = Join-Path $Root "ideas\IDEA-SELFTEST-B-SEED0"
New-Item -ItemType Directory -Force -Path (Join-Path $tempIdea "in"),(Join-Path $tempIdea "out"),(Join-Path $tempIdea "logs") | Out-Null
Set-Content -LiteralPath (Join-Path $tempIdea "idea.txt") -Value "и или но это как что для между если" -Encoding UTF8
if (Test-Path (Join-Path $tempIdea "out\structured_idea.json")) { Remove-Item -LiteralPath (Join-Path $tempIdea "out\structured_idea.json") -Force }

& $py $module --idea $tempIdea --mode BALANCED
$rcStop = $LASTEXITCODE
if ($rcStop -eq 0) { Fail "Expected non-zero code for seed stop, got 0" }
if ($rcStop -ne 2) { Fail "Expected code 2 for seed stop, got $rcStop" }
if (-not (Test-Path (Join-Path $tempIdea "out\llm_prompt_B_anchors.txt"))) { Fail "Missing llm_prompt_B_anchors.txt for seed stop" }
if (-not (Test-Path (Join-Path $tempIdea "in\llm_response_B_anchors.json"))) { Fail "Missing llm_response_B_anchors.json template for seed stop" }
Ok "seed=0 stop behavior validated"

Write-Host "Selfcheck B complete"
