<#
=============================================================================
 rebuild_dh-env.ps1  —  fix the "No Python at ..." problem for good.

 WHY: your dh-env venv is a thin shim that points at a base Python uv installed
 under  %APPDATA%\Roaming\uv\python\...  . On this managed machine, AppData\Roaming
 is REDIRECTED for your interactive session, so your shell can't see that Python
 and every launch dies with "No Python at ...". The files aren't corrupt — they're
 just in a folder your shell can't reach.

 THE FIX: reinstall the base Python into a NON-redirected folder inside your
 project (pyruntime\), rebuild the venv at the same path (C:\Users\birch\dh-env),
 and reinstall the exact same packages. Nothing else in your project changes —
 config.py, the notebook, and your run commands keep working unchanged.

 RUN THIS ONLY when nothing is using dh-env (i.e. the embeddings job has finished
 and no R/Python session is holding it). From a fresh PowerShell:

     powershell -ExecutionPolicy Bypass -File "C:\Users\birch\Rachel\GMU\Dissertation\textanalysis\Claudepythonexperimentadvtext_RSM\env_setup\rebuild_dh-env.ps1"

 Takes ~10-15 min (mostly downloading torch). Safe to re-run if interrupted.
=============================================================================
#>

$ErrorActionPreference = 'Stop'

# ---- Config (edit only if your layout differs) -----------------------------
$proj    = "C:\Users\birch\Rachel\GMU\Dissertation\textanalysis\Claudepythonexperimentadvtext_RSM"
$uv      = "C:\Users\birch\.local\bin\uv.exe"
$runtime = Join-Path $proj "pyruntime"               # NON-redirected Python home (your shell can see this)
$venv    = "C:\Users\birch\dh-env"                   # keep same venv path -> all existing references still work
$reqs    = Join-Path $proj "env_setup\requirements_dh-env.txt"
$pyver   = "3.11.15"
$baseDir = "cpython-$pyver-windows-x86_64-none"

function Step($msg) { Write-Host "`n=== $msg ===" -ForegroundColor Cyan }

# ---- 0. Pre-flight ----------------------------------------------------------
Step "Pre-flight checks"
if (-not (Test-Path $uv))   { throw "uv not found at $uv. Install uv first (https://docs.astral.sh/uv/), then re-run." }
if (-not (Test-Path $reqs)) { throw "Lockfile not found at $reqs." }
# Refuse to run if a dh-env python is still alive (e.g. the embeddings job).
$inUse = Get-Process -Name python -ErrorAction SilentlyContinue |
         Where-Object { $_.Path -like "$venv*" }
if ($inUse) {
  throw "A Python process from $venv is still running (PID $($inUse.Id)). Let it finish, then re-run."
}
Write-Host "uv: $uv"
Write-Host "Reinstalling base Python into: $runtime  (visible, non-redirected)"

# ---- 1. Install CPython into a non-redirected location ----------------------
Step "Installing CPython $pyver into $runtime"
$env:UV_PYTHON_INSTALL_DIR = $runtime
& $uv python install $pyver
$base = Join-Path $runtime "$baseDir\python.exe"
if (-not (Test-Path $base)) { throw "Base Python not found at $base after install." }
Write-Host "Base interpreter: $base"

# ---- 2. Rebuild the venv at the same path, pointing at the visible base ------
Step "Rebuilding venv at $venv"
if (Test-Path $venv) {
  try { Remove-Item -Recurse -Force $venv }
  catch { throw "Could not remove old $venv (a process may be using it). Close R/Python sessions and re-run. $_" }
}
& $uv venv $venv --python $base

# ---- 3. Reinstall the exact package set -------------------------------------
Step "Installing packages from $reqs (this is the slow part — torch download)"
$venvPy = Join-Path $venv "Scripts\python.exe"
& $uv pip install --python $venvPy --extra-index-url https://download.pytorch.org/whl/cpu -r $reqs

# ---- 4. Verify --------------------------------------------------------------
Step "Verifying the rebuilt environment"
& $venvPy -c "import sys; print('python:', sys.version); print('base_prefix:', sys.base_prefix)"
& $venvPy -c "import torch, transformers, sentence_transformers, bertopic, pandas, pyarrow, sklearn, numba, umap; print('ALL KEY IMPORTS OK')"

Write-Host "`n============================================================" -ForegroundColor Green
Write-Host " DONE. dh-env now uses a Python under pyruntime\ that your shell can see." -ForegroundColor Green
Write-Host " Test it yourself in THIS shell:" -ForegroundColor Green
Write-Host "   C:\Users\birch\dh-env\Scripts\python.exe --version" -ForegroundColor Green
Write-Host " Then run any pipeline script, e.g.:" -ForegroundColor Green
Write-Host "   C:\Users\birch\dh-env\Scripts\python.exe $proj\scripts\word_geometry_embeddings.py 18" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
