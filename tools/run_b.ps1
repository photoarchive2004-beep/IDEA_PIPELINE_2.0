param(
  [string]$IdeaDir = "",
  [ValidateSet("BALANCED","FOCUSED","WIDE")]
  [string]$Mode = "BALANCED"
)

$ErrorActionPreference = "Stop"
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

function Find-Prompt([string]$outDir){
  $p = Join-Path $outDir "llm_prompt_B_anchors.txt"
  if (Test-Path $p) { return $p }
  $cand = Get-ChildItem -LiteralPath $outDir -File -ErrorAction SilentlyContinue |
          Where-Object { $_.Name -match '(?i)prompt' } |
          Sort-Object LastWriteTime -Descending |
          Select-Object -First 1
  if ($cand) { return $cand.FullName }
  return $null
}

try {
  $IdeaDir = Resolve-IdeaDir $IdeaDir
  $IdeaDir = (Resolve-Path $IdeaDir).Path
  $ideaName = Split-Path $IdeaDir -Leaf

  $hasIdea = Ensure-IdeaLayout $IdeaDir

  $py = Join-Path $Root ".venv\Scripts\python.exe"
  $module = Join-Path $Root "tools\module_b_lit_scout.py"
  $req = Join-Path $Root "tools\requirements_b.txt"
  if (-not (Test-Path $py))     { throw "Не найден .venv. Сначала запусти 0_SETUP.bat" }
  if (-not (Test-Path $module)) { throw "Не найден tools\module_b_lit_scout.py" }

  Say "Stage B: проверяю зависимости..."
  Log "[CMD] $py -m pip install -r $req"
  & $py -m pip install -r $req *> $Log

  if (-not $hasIdea) {
    Say ""
    Say "⚠️ Не найден idea.txt. Заполни in\idea.txt и запусти RUN_B.bat снова."
    exit 0
  }

  Say "Stage B: выполняю (идея: $ideaName, mode: $Mode)..."
  Log "[CMD] $py $module --idea `"$IdeaDir`" --mode $Mode"
  & $py $module --idea $IdeaDir --mode $Mode *> $Log
  $rc = $LASTEXITCODE

  if ($rc -eq 0) {
    Say ""
    Say "✅ Stage B готова."
    Say "Проверь: out\\corpus.csv, out\\stageB_summary.txt, out\\search_log_B.json"
    exit 0
  }

  if ($rc -eq 2) {
    $prompt = Find-Prompt (Join-Path $IdeaDir "out")
    $resp = Join-Path $IdeaDir "in\llm_response_B_anchors.json"
    $summary = Join-Path $IdeaDir "out\stageB_summary.txt"
    $stopReason = ""
    if (Test-Path $summary) {
      $match = Select-String -LiteralPath $summary -Pattern '^STOP_REASON\s*=\s*(.+)$' | Select-Object -Last 1
      if ($match) { $stopReason = $match.Matches[0].Groups[1].Value.Trim() }
    }
    if (-not (Test-Path $resp)) { New-Item -ItemType File -Force -Path $resp | Out-Null }

    Say ""
    if ($stopReason -eq "llm_already_used_need_edit") {
      Say "Второй запрос в ChatGPT не делаем."
      Say "Отредактируй in\llm_response_B_anchors.json и запусти RUN_B.bat снова."
    } else {
      Say "Нужен ChatGPT (1 раз) для Stage B:"
      Say "1) Откроются prompt и файл ответа."
      Say "2) Prompt уже в буфере обмена (Ctrl+V в ChatGPT)."
      Say "3) Скопируй только JSON-ответ."
      Say "4) Вставь JSON в in\llm_response_B_anchors.json и сохрани."
      Say "5) Запусти RUN_B.bat ещё раз."
    }

    if ($prompt -and $stopReason -ne "llm_already_used_need_edit") {
      Set-Clipboard -Value (Get-Content -Raw -LiteralPath $prompt)
      Start-Process notepad.exe -ArgumentList $prompt | Out-Null
    }
    Start-Process notepad.exe -ArgumentList $resp | Out-Null
    exit 2
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
