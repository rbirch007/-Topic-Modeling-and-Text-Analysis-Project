@echo off
title RSM1935-1950 advanced compute
echo ================================================================
echo  Advanced Python compute  -  scenario RSM1935-1950  (K=15)
echo  Leave this window open until you see DONE at the bottom.
echo ================================================================
echo.
set ADV_SCENARIO=RSM1935-1950
cd /d "C:\Users\birch\Rachel\GMU\Dissertation\textanalysis\Claudepythonexperimentadvtext_RSM"

echo ----- STEP 1/3: embed_documents (the long one, ~10-20 min) -----
"C:\Users\birch\dh-env\Scripts\python.exe" scripts\embed_documents.py 15
echo.
echo ----- STEP 2/3: run_bertopic -----
"C:\Users\birch\dh-env\Scripts\python.exe" scripts\run_bertopic.py 15
echo.
echo ----- STEP 3/3: semantic_change -----
"C:\Users\birch\dh-env\Scripts\python.exe" scripts\semantic_change.py 15
echo.
echo ================================================================
echo  DONE - RSM1935-1950 complete. You can close this window.
echo ================================================================
pause
