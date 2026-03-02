param(
  [Parameter(Mandatory=$true)][string]$Root,
  [Parameter(Mandatory=$true)][string]$LogPath
)

# PowerShell 5.1 friendly
$ErrorActionPreference = "Stop"

function Write-Log {
  param(
    [Parameter(Mandatory=$true)][string]$Level,
    [Parameter(Mandatory=$true)][string]$Message
  )
  $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  $line = "[{0}] [{1}] {2}" -f $ts, $Level, $Message
  Write-Host $line
  $line | Out-File -FilePath $LogPath -Append -Encoding UTF8
}

function Exec {
  param(
    [Parameter(Mandatory=$true)][string]$Exe,
    [Parameter()][string[]]$Args = @(),
    [int]$TimeoutSec = 0
  )

  # Human-readable log only
  $pretty = ($Args | ForEach-Object {
    if ($_ -match "[\s]") { '"' + ($_ -replace '"','`"') + '"' } else { $_ }
  }) -join " "
  Write-Log "DIAG" ("RUN: ""{0}"" {1}" -f $Exe, $pretty)

  $stdout = [System.IO.Path]::GetTempFileName()
  $stderr = [System.IO.Path]::GetTempFileName()

  try {
    $p = Start-Process -FilePath $Exe -ArgumentList $Args -NoNewWindow -PassThru `
      -RedirectStandardOutput $stdout -RedirectStandardError $stderr

    if ($TimeoutSec -gt 0) {
      $ok = $p.WaitForExit($TimeoutSec * 1000)
      if (-not $ok) {
        try { $p.Kill() } catch {}
        $out = Get-Content -Raw -ErrorAction SilentlyContinue $stdout
        $err = Get-Content -Raw -ErrorAction SilentlyContinue $stderr
        if ($null -eq $out) { $out = "" }
        if ($null -eq $err) { $err = "" }
        return @{ ExitCode = 124; StdOut = [string]$out; StdErr = [string]$err; TimedOut = $true }
      }
    }

    $p.WaitForExit() | Out-Null
    $out2 = Get-Content -Raw -ErrorAction SilentlyContinue $stdout
    $err2 = Get-Content -Raw -ErrorAction SilentlyContinue $stderr
    if ($null -eq $out2) { $out2 = "" }
    if ($null -eq $err2) { $err2 = "" }
    return @{ ExitCode = [int]$p.ExitCode; StdOut = [string]$out2; StdErr = [string]$err2; TimedOut = $false }
  }
  finally {
    Remove-Item -Force -ErrorAction SilentlyContinue $stdout, $stderr | Out-Null
  }
}

function Parse-VersionFromText {
  param([string]$Text)
  if ([string]::IsNullOrWhiteSpace($Text)) { return $null }
  $t = ($Text -replace "[`r`n]"," ").Trim()
  $m = [regex]::Match($t, "Python\s+(\d+)\.(\d+)")
  if (-not $m.Success) { return $null }
  return @{ Major = [int]$m.Groups[1].Value; Minor = [int]$m.Groups[2].Value; Str = ("{0}.{1}" -f $m.Groups[1].Value, $m.Groups[2].Value) }
}

function Get-PythonVersion {
  param([string]$PyExe)
  if (-not (Test-Path $PyExe)) { return $null }

  # Skip Microsoft Store shims
  if ($PyExe -match "\\WindowsApps\\python(\d*)\.exe$") { return $null }
  if ($PyExe -match "\\WindowsApps\\python3\.exe$") { return $null }

  $r = Exec -Exe $PyExe -Args @("--version") -TimeoutSec 15
  if ($r.TimedOut) {
    Write-Log "WARN" ("Timeout while checking version: {0}" -f $PyExe)
    return $null
  }
  if ($r.ExitCode -ne 0) {
    Write-Log "WARN" ("Version check failed for {0}: {1}" -f $PyExe, ([string]$r.StdErr).Trim())
    return $null
  }

  # python --version sometimes prints to stderr
  $txt = ([string]$r.StdOut + " " + [string]$r.StdErr)
  $v = Parse-VersionFromText -Text $txt
  if ($null -eq $v) {
    Write-Log "WARN" ("Could not parse version output for {0}: {1}" -f $PyExe, $txt.Trim())
  }
  return $v
}

function Find-PythonViaPyLauncher {
  $py = "$env:WINDIR\py.exe"
  if (-not (Test-Path $py)) { return @() }

  $r = Exec -Exe $py -Args @("-0p") -TimeoutSec 15
  if ($r.ExitCode -ne 0 -or $r.TimedOut) { return @() }

  $lines = ($r.StdOut -split "`r?`n") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
  $paths = New-Object System.Collections.Generic.List[string]
  foreach ($ln in $lines) {
    $m = [regex]::Match($ln, "(-\d+\.\d+)[^\s]*\s+(.+python\.exe)$")
    if ($m.Success) {
      $p = $m.Groups[2].Value.Trim()
      if ($p -and (Test-Path $p)) { $paths.Add($p) | Out-Null }
    }
  }
  return ($paths | Select-Object -Unique)
}

function Find-PythonCandidates {
  $cands = New-Object System.Collections.Generic.List[string]

  # Optional override (zero routine; only if user sets env var)
  if ($env:IDEA_PY_EXE) {
    $p = $env:IDEA_PY_EXE.Trim()
    if ($p -and (Test-Path $p)) { $cands.Add($p) | Out-Null }
  }

  foreach ($p in (Find-PythonViaPyLauncher)) { $cands.Add($p) | Out-Null }

  foreach ($cmd in @("python.exe", "python3.exe")) {
    $w = (Get-Command $cmd -ErrorAction SilentlyContinue)
    if ($w -and (Test-Path $w.Source)) { $cands.Add($w.Source) | Out-Null }
  }

  $common = @(
    "$env:ProgramFiles\Python312\python.exe",
    "$env:ProgramFiles\Python311\python.exe",
    "$env:ProgramFiles\Python310\python.exe",
    "$env:LocalAppData\Programs\Python\Python312\python.exe",
    "$env:LocalAppData\Programs\Python\Python311\python.exe",
    "$env:LocalAppData\Programs\Python\Python310\python.exe"
  )
  foreach ($p in $common) { if (Test-Path $p) { $cands.Add($p) | Out-Null } }

  return ($cands | Select-Object -Unique)
}

function Select-BestPython {
  param([string[]]$Candidates)

  $items = @()
  foreach ($p in $Candidates) {
    $v = Get-PythonVersion -PyExe $p
    if ($null -ne $v) {
      Write-Log "DIAG" ("Candidate OK: {0} => {1}" -f $p, $v.Str)
      $items += [pscustomobject]@{ Path=$p; Major=$v.Major; Minor=$v.Minor; VerStr=$v.Str }
    } else {
      Write-Log "DIAG" ("Candidate SKIP: {0}" -f $p)
    }
  }

  if (-not $items -or $items.Count -eq 0) { return $null }

  $items = $items | Where-Object { $_.Major -eq 3 -and $_.Minor -ge 10 }
  if (-not $items -or $items.Count -eq 0) { return $null }

  $pref = $items | Where-Object { $_.Minor -in 12,11,10 } | Sort-Object Minor -Descending
  if ($pref -and $pref.Count -gt 0) { return $pref[0] }

  return ($items | Sort-Object Minor -Descending)[0]
}

try {
  Write-Log "INFO" "=== 0_SETUP start ==="
  Write-Log "DIAG" ("Root={0}" -f $Root)
  Write-Log "DIAG" ("PowerShell={0}" -f $PSVersionTable.PSVersion.ToString())

  $env:PYTHONUTF8 = "1"
  $env:PYTHONIOENCODING = "utf-8"

  $cands = Find-PythonCandidates
  if (-not $cands -or $cands.Count -eq 0) {
    Write-Log "ERROR" "Python not found. Install Python 3.11/3.12 and retry."
    exit 2
  }

  Write-Log "DIAG" ("Python candidates: {0}" -f ($cands -join " | "))

  $best = Select-BestPython -Candidates $cands
  if ($null -eq $best) {
    Write-Log "ERROR" "No suitable Python 3.10+ found. Install Python 3.11/3.12 and retry."
    exit 2
  }

  Write-Log "OK" ("Selected Python: {0} (v{1})" -f $best.Path, $best.VerStr)

  $venvDir = Join-Path $Root ".venv"
  $venvPy  = Join-Path $venvDir "Scripts\python.exe"

  $needCreate = $true
  if (Test-Path $venvPy) {
    $vr = Exec -Exe $venvPy -Args @("--version") -TimeoutSec 15
    if ($vr.ExitCode -eq 0) { $needCreate = $false }
  }

  if ($needCreate) {
    if (Test-Path $venvDir) {
      Write-Log "WARN" ".venv exists but looks broken; recreating"
      Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $venvDir
    }
    $rVenv = Exec -Exe $best.Path -Args @("-m","venv",$venvDir) -TimeoutSec 120
    if ($rVenv.ExitCode -ne 0) {
      Write-Log "ERROR" ("Failed to create venv. {0}" -f ([string]$rVenv.StdErr).Trim())
      exit 3
    }
  }

  Write-Log "INFO" "Upgrading pip/setuptools/wheel..."
  $rPip = Exec -Exe $venvPy -Args @("-m","pip","install","-U","pip","setuptools","wheel") -TimeoutSec 600
  if ($rPip.ExitCode -ne 0) {
    Write-Log "ERROR" ("pip upgrade failed. {0}" -f ([string]$rPip.StdErr).Trim())
    exit 4
  }

  $req = Join-Path $Root "requirements.txt"
  if (Test-Path $req) {
    Write-Log "INFO" ("Installing requirements from {0}" -f $req)
    $rReq = Exec -Exe $venvPy -Args @("-m","pip","install","-r",$req) -TimeoutSec 1800
    if ($rReq.ExitCode -ne 0) {
      Write-Log "ERROR" ("requirements install failed. {0}" -f ([string]$rReq.StdErr).Trim())
      exit 5
    }
  } else {
    Write-Log "WARN" "requirements.txt not found; skipping dependency install."
  }

  Write-Log "OK" "Setup finished successfully."
  exit 0
}
catch {
  Write-Log "ERROR" ("Unhandled error: {0}" -f $_.Exception.Message)
  exit 99
}
