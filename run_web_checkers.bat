@echo off
rem ------------------------------------------------------
rem Batch script to start a local HTTP server and open
rem the 3D Checkers game in the default browser.
rem ------------------------------------------------------

rem Change to the script directory
cd /d "%~dp0"

rem Start the HTTP server in a new window
echo Starting HTTP server on port 8000...
start "Checkers Server" cmd /k "python -m http.server 8000"

rem Wait a moment for the server to initialize
timeout /t 2 >nul

rem Open the game in the default browser
echo Opening 3D Checkers in your browser...
start "" "http://localhost:8000/index.html"

echo.
echo Server is running. To stop it, close the "Checkers Server" window.
pause