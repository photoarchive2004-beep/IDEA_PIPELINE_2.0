param(
  [string]$IdeaDir = "",
  [ValidateSet("BALANCED","FOCUSED","WIDE")]
  [string]$Mode = "BALANCED",
  [ValidateSet("balanced","wide","focused")][string]$Scope = "",
  [int]$N = 300,
  [switch]$CleanHard
)

if ($Scope -and $Scope.Trim()) { $Mode = $Scope.ToUpperInvariant() }

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
$OutputEncoding = [Text.UTF8Encoding]::new($false)
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
if (-not $env:STAGE_B1_LLM_LIMIT -or [string]::IsNullOrWhiteSpace($env:STAGE_B1_LLM_LIMIT)) {
  $env:STAGE_B1_LLM_LIMIT = "10"
}
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$LogDir = Join-Path $Root "launcher_logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$Log = Join-Path $LogDir "runB_last.log"
"" | Out-File -FilePath $Log -Encoding UTF8

function Say([string]$s){ Write-Host $s }
function Log([string]$s){ $s | Out-File -FilePath $Log -Append -Encoding UTF8 }

function Read-SummaryValue([string]$path, [string]$key, [string]$fallback="") {
  if (-not (Test-Path $path)) { return $fallback }
  $m = Select-String -LiteralPath $path -Pattern "^$key\s*=\s*(.+)$" | Select-Object -Last 1
  if ($m) { return $m.Matches[0].Groups[1].Value.Trim() }
  return $fallback
}

function Resolve-IdeaDir([string]$arg) {
  $ideas = Join-Path $Root "ideas"
  if (-not (Test-Path $ideas)) { throw "Нет папки ideas. Сначала запусти 1_NEW_IDEA.bat" }

  if ([string]::IsNullOrWhiteSpace($arg)) {
    $active = Join-Path $ideas "_ACTIVE_PATH.txt"
    if (Test-Path $active) {
      $p = (Get-Content -LiteralPath $active -Raw).Trim()
      if ($p) {
        if (-not [IO.Path]::IsPathRooted($p)) { $p = Join-Path $Root $p }
        $p = [IO.Path]::GetFullPath($p)
        if (Test-Path $p) { return $p }
      }
    }
  }

  if ($arg -and $arg.Trim()) {
    $p = $arg
    if (-not [IO.Path]::IsPathRooted($p)) { $p = Join-Path $Root $p }
    $p = [IO.Path]::GetFullPath($p)
    if ((Split-Path -Leaf $p) -ieq "out") { $p = Split-Path -Parent $p }
    if (-not (Test-Path $p)) { throw "Папка идеи не найдена: $p" }
    return $p
  }

  $cands = Get-ChildItem -LiteralPath $ideas -Directory -ErrorAction SilentlyContinue |
           Where-Object { $_.Name -like "IDEA-*" } |
           Sort-Object Name -Descending
  $first = $cands | Select-Object -First 1
  if ($first) { return $first.FullName }

  throw "В папке ideas нет ни одной IDEA-*"
}

function Ensure-IdeaLayout([string]$ideaDir){
  $inDir = Join-Path $ideaDir "in"
  $outDir = Join-Path $ideaDir "out"
  $logsDir = Join-Path $ideaDir "logs"
  New-Item -ItemType Directory -Force -Path $inDir,$outDir,$logsDir | Out-Null

  $ideaTop = Join-Path $ideaDir "idea.txt"
  $ideaIn  = Join-Path $inDir "idea.txt"
  if (Test-Path $ideaIn) {
    if ((-not (Test-Path $ideaTop)) -or ((Get-Item $ideaIn).LastWriteTime -gt (Get-Item $ideaTop).LastWriteTime)) {
      Copy-Item -Force -LiteralPath $ideaIn -Destination $ideaTop
    }
  }
  if (-not (Test-Path $ideaTop)) { return $false }
  return $true
}

function Clear-StageBOutputs([string]$outDir) {
  if (-not (Test-Path $outDir)) { return }
  $files = @(
    "corpus.csv","corpus_all.csv","corpus_support.csv","corpus_support_all.csv",
    "corpus_background.csv","stageB_summary.txt","stageB1_summary.txt",
    "search_log_B.json","search_log.json","search_strategy_B.md","prisma_lite.md","prisma_lite_B.md","llm_requests_B1.json"
  )
  foreach ($name in $files) {
    $p = Join-Path $outDir $name
    if (Test-Path $p) { Remove-Item -LiteralPath $p -Force -ErrorAction SilentlyContinue }
  }
  Get-ChildItem -LiteralPath $outDir -Filter "llm_prompt_B*_*.txt" -ErrorAction SilentlyContinue |
    Remove-Item -Force -ErrorAction SilentlyContinue
}

try {
  $IdeaDir = Resolve-IdeaDir $IdeaDir
  $IdeaDir = (Resolve-Path $IdeaDir).Path
  $ideaName = Split-Path $IdeaDir -Leaf
  $OutDir = Join-Path $IdeaDir "out"
  $PromptPath = Join-Path $OutDir "llm_prompt_B1_anchors.txt"
  $RespPath = Join-Path (Join-Path $IdeaDir "in") "llm_response_B1_anchors.json"
  $SummaryPath = Join-Path $OutDir "stageB1_summary.txt"
  if (-not (Test-Path $SummaryPath)) { $SummaryPath = Join-Path $OutDir "stageB_summary.txt" }

  $hasIdea = Ensure-IdeaLayout $IdeaDir

  $py = Join-Path $Root ".venv\Scripts\python.exe"
  $module = Join-Path $Root "tools\b_lit_scout.py"
  if (-not (Test-Path $module)) { $module = Join-Path $Root "tools\module_b_lit_scout.py" }
  $req = Join-Path $Root "tools\requirements_b.txt"
  if (-not (Test-Path $py))     { throw "Не найден .venv. Сначала запусти 0_SETUP.bat" }
  if (-not (Test-Path $module)) { throw "Не найден tools\module_b_lit_scout.py" }

  Say "Stage B1: проверяю зависимости..."
  Log "[CMD] $py -m pip install -r $req"
  & $py -m pip install -r $req *>> $Log

  if (-not $hasIdea) {
    Say ""
    Say "⚠️ Не найден idea.txt. Заполни in\idea.txt и запусти RUN_B.bat снова."
    exit 0
  }

  Say "Stage B1: выполняю (идея: $ideaName, mode: $Mode, N: $N)..."
  $cmdArgs = @("--idea-dir", $IdeaDir, "--scope", $($Mode.ToLowerInvariant()), "--n", "$N", "--clean-out")
  if ($CleanHard) { $cmdArgs += "--clean-hard" }
  Log "[CMD] $py $module $($cmdArgs -join ' ')"
  & $py $module @cmdArgs *>> $Log
  $rc = $LASTEXITCODE

  $statusValue = Read-SummaryValue $SummaryPath "STATUS" ""
  $stopReason = Read-SummaryValue $SummaryPath "STOP_REASON" ""
  $promptFile = Read-SummaryValue $SummaryPath "PROMPT_FILE" $PromptPath
  $waitFile = Read-SummaryValue $SummaryPath "WAIT_FILE" $RespPath

  if (($rc -eq 2) -or ($statusValue -eq "WAITING_FOR_LLM")) {
    Say ""
    Say "⏸️ Stage B1 ждёт JSON-ответ ChatGPT."
    Say "PROMPT_FILE: $promptFile"
    Say "WAIT_FILE:   $waitFile"
    if (Test-Path $promptFile) {
      Start-Process notepad.exe -ArgumentList $promptFile | Out-Null
      Set-Clipboard -Value (Get-Content -LiteralPath $promptFile -Raw)
      } else {
      Say "⚠️ PROMPT_FILE не найден: $promptFile"
    }
    Say "PROMPT скопирован в буфер обмена. Вставьте его в ChatGPT и сохраните ответ JSON в WAIT_FILE."
    Read-Host "Нажмите Enter после сохранения JSON" | Out-Null
    if (Test-Path $waitFile) {
      Say "JSON найден. Перезапускаю Stage B1..."
      & $py $module @cmdArgs *>> $Log
      $rc = $LASTEXITCODE
      $statusValue = Read-SummaryValue $SummaryPath "STATUS" "DEGRADED"
    }
  }

  if ($rc -eq 0) {
    Say ""
    Say "✅ Stage B1 завершена ($statusValue)."
    Say "Файлы: out\corpus.csv, out\corpus_all.csv, out\stageB1_summary.txt, out\search_log.json"
    exit 0
  }

  Say ""
  Say "❌ Stage B1: ошибка (STOP_REASON=$stopReason). Открою лог."
  Start-Process notepad.exe -ArgumentList $Log | Out-Null
  exit 1
}
catch {
  Say ""
  Say "❌ Stage B1: ошибка запуска. Открою лог."
  $_ | Out-String | Out-File -FilePath $Log -Append -Encoding UTF8
  Start-Process notepad.exe -ArgumentList $Log | Out-Null
  exit 1
}
