# How to run the advanced-analysis Python compute (step by step)

Goal: for one scenario (e.g. `RSM1935-1950`), run the three Python scripts that
produce the embeddings / BERTopic / diachronic artifacts.

GOLDEN RULES
- Always call Python by its FULL PATH: `C:\Users\birch\dh-env\Scripts\python.exe`
  Never just `python` (that's Python 3.14 / a Store stub with no ML packages).
- TYPE the commands. Do NOT copy-paste from chat — pasting can turn straight
  quotes (") into "smart quotes" that break the command.
- None of the paths have spaces, so you do NOT need quotes around them.

------------------------------------------------------------------------
STEP 0 — Open a BRAND-NEW PowerShell window
------------------------------------------------------------------------
Close any PowerShell you already had open (it may hold broken leftover state).
Start menu -> type:  PowerShell  -> press Enter.
(Use "Windows PowerShell", the blue one. Not the RStudio Terminal for now.)

------------------------------------------------------------------------
STEP 1 — Confirm you are actually in PowerShell
------------------------------------------------------------------------
Type:
    $PSVersionTable.PSVersion

EXPECT: a version table (e.g. Major 5 ...). If you get "command not found",
you're not in PowerShell — close and reopen as in Step 0.

------------------------------------------------------------------------
STEP 2 — Confirm the Python file exists
------------------------------------------------------------------------
Type:
    Test-Path C:\Users\birch\dh-env\Scripts\python.exe

EXPECT: True
If False -> stop and tell me; the env moved.

------------------------------------------------------------------------
STEP 3 — THE KEY TEST: does that Python run?  (type it, no quotes)
------------------------------------------------------------------------
Type:
    C:\Users\birch\dh-env\Scripts\python.exe --version

EXPECT: Python 3.11.15

>>> If you see "No Python at ..." or anything else here, STOP. <<<
Copy the EXACT output and send it to me. Nothing past this point matters
until this line prints "Python 3.11.15".

------------------------------------------------------------------------
STEP 4 — Tell the scripts which scenario (type the quotes by hand)
------------------------------------------------------------------------
Type (the only quotes you need — type them, don't paste):
    $env:ADV_SCENARIO = "RSM1935-1950"

Verify it took (should print RSM1935-1950 with NO quote marks):
    echo $env:ADV_SCENARIO

------------------------------------------------------------------------
STEP 5 — Go to the project folder
------------------------------------------------------------------------
Type:
    cd C:\Users\birch\Rachel\GMU\Dissertation\textanalysis\Claudepythonexperimentadvtext_RSM

------------------------------------------------------------------------
STEP 6 — Run the long one: embed_documents  (this is ~10-20 min)
------------------------------------------------------------------------
Type:
    C:\Users\birch\dh-env\Scripts\python.exe scripts\embed_documents.py 15

EXPECT lines like:
    [embed] K=15  docs=5693  model=all-MiniLM-L6-v2
    [embed] checkpoint: 500/5693 ...
It is working when the checkpoint numbers climb. If it stops partway, just
run the SAME line again -- it resumes from the last checkpoint.

------------------------------------------------------------------------
STEP 7 — Run the quick two (a minute or two each)
------------------------------------------------------------------------
    C:\Users\birch\dh-env\Scripts\python.exe scripts\run_bertopic.py 15
    C:\Users\birch\dh-env\Scripts\python.exe scripts\semantic_change.py 15

(semantic_change downloads a model the first time -- that's normal.)

------------------------------------------------------------------------
STEP 8 — Done. Artifacts land here:
------------------------------------------------------------------------
    output_advanced_RSM\RSM1935-1950\{embeddings,bertopic,diachronic}\
Then knit RSMadvancedtextanalysis.Rmd with:
    scenario <- "RSM1935-1950"   and   pin_K <- 15L

========================================================================
THE "K" FOR EACH SCENARIO (the number at the end of each command)
========================================================================
  RSMfull        18      (RSM project)
  RSM1914-1934   15      (RSM project)
  RSM1935-1950   15      (RSM project)
  RSM1951-1970   18      (RSM project)
  WEfull         35      (WE project: ...\Claudepythonexperimentadvtext)
  WE1872-1891    20      (WE project)
  WE1892-1912    25      (WE project)
For a WE scenario: same steps, but `cd` to the WE project folder and use its K.
========================================================================
