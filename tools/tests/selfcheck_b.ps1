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

if (-not $searchLog.token_probe -or $searchLog.token_probe.Count -lt 1) { Fail "search_log_B.json missing token_probe" }
Ok "token_probe exists"

$planned = @($searchLog.planned_queries)
if ($planned.Count -gt 8) { Fail "planned_queries > 8" }
Ok "planned_queries <= 8"

$stopwords = @("and","or","the","a","an","of","for","in","on","to","from","with","without","by","via","using","use","based","study","studies","result","results","method","methods","analysis","analyses","data","model","models","approach","approaches","review","reviews","и","или","в","на","по","для","от","с","без","как","что","это","эти","тот","та","те","также","метод","методы","анализ","данные","модель","модели","обзор")
$stopset = @{}
$stopwords | ForEach-Object { $stopset[$_] = $true }
$normSeen = @{}
foreach($q in $planned){
  $tokens = [regex]::Matches($q, "[A-Za-zА-Яа-я0-9\-]+") | ForEach-Object { $_.Value.ToLower() }
  foreach($tok in $tokens){ if (($tok -ne "and") -and ($tok -ne "or") -and $stopset.ContainsKey($tok)) { Fail "planned query has stopword token '$tok': $q" } }
  $nq = ([regex]::Replace($q.ToLower(), "\s+", " ")).Trim()
  if ($normSeen.ContainsKey($nq)) { Fail "Duplicate planned query detected: $q" }
  $normSeen[$nq] = $true
}
Ok "planned queries have no stopword terms and no duplicates"

$tooBroad = @{}
foreach($tp in @($searchLog.token_probe)){
  if ($tp.category -eq "TOO_BROAD") { $tooBroad[$tp.token.ToLower()] = $true }
}
foreach($q in $planned){
  $tokens = [regex]::Matches($q, "[A-Za-z0-9\-]+") | ForEach-Object { $_.Value.ToLower() }
  if ($tokens.Count -eq 1 -and $tooBroad.ContainsKey($tokens[0])) { Fail "Found planned too_broad solo query: $q" }
}
Ok "no too_broad solo queries"

if (-not ($searchLog.PSObject.Properties.Name -contains "rejected_queries")) { Fail "rejected_queries field missing" }
Ok "rejected_queries logging exists"

$llmPromptsCreated = 0
if ($searchLog.stats -and ($searchLog.stats.PSObject.Properties.Name -contains "llm_prompts_created")) {
  $llmPromptsCreated = [int]$searchLog.stats.llm_prompts_created
}
if ($llmPromptsCreated -gt 1) { Fail "llm_prompts_created must be <= 1, got $llmPromptsCreated" }
Ok "llm prompt budget respected (<=1)"

if ($searchLog.stats -and ($searchLog.stats.PSObject.Properties.Name -contains "go_nogo")) {
  $go = [string]$searchLog.stats.go_nogo
  if ($go -eq "GO") {
    if (-not (Test-Path (Join-Path $idea "out\search_strategy_B.md"))) { Fail "Missing out/search_strategy_B.md for GO run" }
    Ok "search_strategy_B.md exists for GO run"
  } else {
    if ($rc -ne 2) { Fail "NO-GO run must stop with exit code 2" }
    $hasPrompt = Test-Path (Join-Path $idea "out\llm_prompt_B_anchors.txt")
    $stopReason = ""
    if ($searchLog.stats.PSObject.Properties.Name -contains "stop_reason") { $stopReason = [string]$searchLog.stats.stop_reason }
    if ((-not $hasPrompt) -and ($stopReason -ne "llm_already_used_need_edit")) { Fail "NO-GO must create prompt or set stop_reason=llm_already_used_need_edit" }
    Ok "NO-GO stop behavior validated"
  }
}

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

# Offline fixture with abstracts should produce support hits and support corpus
$tempFixture = Join-Path $Root "tools\tests\fixtures\tmp_support"
New-Item -ItemType Directory -Force -Path $tempFixture | Out-Null
$fixtureJson = @'
{
  "results": [
    {
      "id": "https://openalex.org/W1",
      "title": "River network connectivity and gene flow in freshwater fish",
      "publication_year": 2021,
      "doi": "https://doi.org/10.1000/test1",
      "type": "article",
      "cited_by_count": 11,
      "primary_location": {"landing_page_url": "https://example.org/1", "source": {"display_name": "Journal A", "type": "journal"}},
      "authorships": [{"author": {"display_name": "A Author"}}],
      "concepts": [{"display_name": "Genetics"}],
      "referenced_works": [],
      "related_works": [],
      "abstract_inverted_index": {"river": [0], "network": [1], "connectivity": [2], "gene": [3], "flow": [4], "in": [5], "freshwater": [6], "fish": [7]}
    },
    {
      "id": "https://openalex.org/W2",
      "title": "HydroRIVERS based genotype-environment association methods",
      "publication_year": 2020,
      "doi": "https://doi.org/10.1000/test2",
      "type": "article",
      "cited_by_count": 9,
      "primary_location": {"landing_page_url": "https://example.org/2", "source": {"display_name": "Journal B", "type": "journal"}},
      "authorships": [{"author": {"display_name": "B Author"}}],
      "concepts": [{"display_name": "Ecology"}],
      "referenced_works": [],
      "related_works": [],
      "abstract_inverted_index": {"hydrorivers": [0], "based": [1], "genotype-environment": [2], "association": [3], "methods": [4]}
    }
  ]
}
'@
Set-Content -LiteralPath (Join-Path $tempFixture "openalex_seed.json") -Value $fixtureJson -Encoding UTF8
& $py $module --idea $idea --mode BALANCED --offline-fixtures $tempFixture
if ($LASTEXITCODE -ne 0 -and $LASTEXITCODE -ne 2) { Fail "Support fixture offline run failed" }
$sum2 = Get-Content -LiteralPath (Join-Path $idea "out\stageB_summary.txt") -Raw
if ($sum2 -match "support_tokens_count = ([0-9]+)" -and $sum2 -match "support_count = ([0-9]+)") {
  $st = [int]$matches[1]
}
$log2 = Get-Content -LiteralPath (Join-Path $idea "out\search_log_B.json") -Raw | ConvertFrom-Json
if ($log2.support_tokens.Count -gt 0) {
  $supportRows = Import-Csv (Join-Path $idea "out\corpus_support.csv")
  if ($supportRows.Count -le 0) { Fail "Expected support rows for fixture with abstracts" }
  Ok "support corpus populated on abstract fixture"
}
Remove-Item -LiteralPath $tempFixture -Recurse -Force

$summary = Get-Content -LiteralPath (Join-Path $idea "out\stageB_summary.txt") -Raw
foreach($required in @("probe_tokens_tested", "too_broad_count", "broad_count", "ok_count", "narrow_count", "zero_count")){
  if ($summary -notmatch [regex]::Escape($required)) { Fail "stageB_summary missing $required" }
}
Ok "stageB_summary probe counters found"

foreach($required in @("llm_budget", "drift_round0")){
  if ($summary -notmatch [regex]::Escape($required)) { Fail "stageB_summary missing $required" }
}
Ok "stageB_summary includes llm/drift rounds"

if ($summary -match "drift_round0 = ([0-9\.]+)") {
  $d0 = [double]$matches[1]
  if ($d0 -gt 0.30 -and $summary -notmatch "auto-fix rounds: [12]") {
    Fail "Expected auto-fix rounds when drift_round0 > target"
  }
}
Ok "auto-fix trigger check passed"

Write-Host "Selfcheck B complete"
