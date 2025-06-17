@echo off
echo === Resetting WSL ===
echo This will shutdown all running WSL instances and restart them.
echo.

echo Step 1: Shutting down WSL...
wsl.exe --shutdown
echo.

echo Step 2: Waiting 5 seconds for complete shutdown...
timeout /t 5 /nobreak >nul
echo.

echo Step 3: Checking WSL status...
wsl.exe --status
echo.

echo WSL has been reset. Now try running fix_wsl_and_run_codex.bat again.
echo.
pause 