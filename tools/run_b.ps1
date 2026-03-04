param(
  [string]$IdeaDir = "",
  [ValidateSet("BALANCED","FOCUSED","WIDE")]
  [string]$Mode = "BALANCED",
  [ValidateSet("balanced","wide","focused")][string]$Scope = "",
  [int]$N = 300
)

if ($Scope -and $Scope.Trim()) { $Mode = $Scope.ToUpperInvariant() }

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
$OutputEncoding = [Text.UTF8Encoding]::new($false)
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$LogDir = Join-Path $Root "launcher_logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$Log = Join-Path $LogDir "runB_last.log"
"" | Out-File -FilePath $Log -Encoding UTF8

function Say([string]$s){ Write-Host $s }
function Log([string]$s){ $s | Out-File -FilePath $Log -Append -Encoding UTF8 }

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

try {
  $IdeaDir = Resolve-IdeaDir $IdeaDir
  $IdeaDir = (Resolve-Path $IdeaDir).Path
  $ideaName = Split-Path $IdeaDir -Leaf
  $OutDir = Join-Path $IdeaDir "out"
  $PromptPath = Join-Path $OutDir "llm_prompt_B_anchors.txt"
  $RespPath = Join-Path (Join-Path $IdeaDir "in") "llm_response_B_anchors.json"
  $SummaryPath = Join-Path $OutDir "stageB_summary.txt"

  $hasIdea = Ensure-IdeaLayout $IdeaDir

  $py = Join-Path $Root ".venv\Scripts\python.exe"
  $module = Join-Path $Root "tools\b_lit_scout.py"
  if (-not (Test-Path $module)) { $module = Join-Path $Root "tools\module_b_lit_scout.py" }
  $req = Join-Path $Root "tools\requirements_b.txt"
  if (-not (Test-Path $py))     { throw "Не найден .venv. Сначала запусти 0_SETUP.bat" }
  if (-not (Test-Path $module)) { throw "Не найден tools\module_b_lit_scout.py" }

  Say "Stage B: проверяю зависимости..."
  Log "[CMD] $py -m pip install -r $req"
  & $py -m pip install -r $req *>> $Log

  if (-not $hasIdea) {
    Say ""
    Say "⚠️ Не найден idea.txt. Заполни in\idea.txt и запусти RUN_B.bat снова."
    exit 0
  }

  Say "Stage B: выполняю (идея: $ideaName, mode: $Mode, N: $N)..."
  Log "[CMD] $py $module --idea-dir `"$IdeaDir`" --scope $($Mode.ToLowerInvariant()) --n $N"
  & $py $module --idea-dir $IdeaDir --scope $($Mode.ToLowerInvariant()) --n $N *>> $Log
  $rc = $LASTEXITCODE

  if ($rc -eq 0) {
    Say ""
    Say "✅ Stage B готова."
    Say "Файлы: out\corpus.csv, out\corpus_all.csv, out\stageB_summary.txt, out\search_log_B.json"
    exit 0
  }

  if ($rc -eq 2) {
    $stopReason = ""

    if (Test-Path $SummaryPath) {
      $match = Select-String -LiteralPath $SummaryPath -Pattern '^STOP_REASON\s*=\s*(.+)$' | Select-Object -Last 1
      if ($match) { $stopReason = $match.Matches[0].Groups[1].Value.Trim() }
    }

    if (-not (Test-Path $RespPath)) { New-Item -ItemType File -Force -Path $RespPath | Out-Null }

    Say ""
    Say "⚠️ Stage B ждёт ручной шаг."

    if ($stopReason -eq "llm_limit_reached_edit_json") {
      Say "Лимит ChatGPT 3/3 уже использован."
      Say "Шаг 1: Откроется файл in\llm_response_B_anchors.json."
      Say "Шаг 2: Впиши или поправь JSON вручную."
      Say "Шаг 3: Сохрани файл."
      Say "Шаг 4: Прочитай подсказку в out\stageB_summary.txt."
      Say "Шаг 5: Запусти RUN_B.bat снова."
      Start-Process notepad.exe -ArgumentList $RespPath | Out-Null
      if (Test-Path $SummaryPath) { Start-Process notepad.exe -ArgumentList $SummaryPath | Out-Null }
      exit 2
    }

    if (-not (Test-Path $PromptPath)) {
      Say "Ожидался prompt, но не создан. Пересоздаю..."
      Log "[CMD] $py $module --idea-dir `"$IdeaDir`" --scope $($Mode.ToLowerInvariant()) --n $N --emit-anchors-prompt-only"
      & $py $module --idea-dir $IdeaDir --scope $($Mode.ToLowerInvariant()) --n $N --emit-anchors-prompt-only *>> $Log
    }

    if (Test-Path $PromptPath) {
      Set-Clipboard -Value (Get-Content -Raw -LiteralPath $PromptPath)
      Say "Шаг 1: Открой ChatGPT."
      Say "Шаг 2: Prompt Stage B уже в буфере обмена. Вставь его в ChatGPT."
      Say "Шаг 3: Скопируй обратно только JSON без текста."
      Say "Шаг 4: Вставь JSON в in\llm_response_B_anchors.json и сохрани."
      Say "Шаг 5: Запусти RUN_B.bat снова."
      Start-Process notepad.exe -ArgumentList $PromptPath | Out-Null
      Start-Process notepad.exe -ArgumentList $RespPath | Out-Null
      if (Test-Path $SummaryPath) { Start-Process notepad.exe -ArgumentList $SummaryPath | Out-Null }
      exit 2
    }

    Say "Ошибка Stage B: не найден файл prompt по пути $PromptPath"
    Say "Открой out\stageB_summary.txt и проверь STOP_REASON."
    if (Test-Path $SummaryPath) { Start-Process notepad.exe -ArgumentList $SummaryPath | Out-Null }
    Start-Process notepad.exe -ArgumentList $RespPath | Out-Null
    exit 1
  }

  Say ""
  Say "❌ Stage B: ошибка. Открою лог."
  Start-Process notepad.exe -ArgumentList $Log | Out-Null
  exit 1
}
catch {
  Say ""
  Say "❌ Stage B: ошибка запуска. Открою лог."
  $_ | Out-String | Out-File -FilePath $Log -Append -Encoding UTF8
  Start-Process notepad.exe -ArgumentList $Log | Out-Null
  exit 1
}
