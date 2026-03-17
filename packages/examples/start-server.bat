@echo off
title Ring Dashboard Server

:: Kill any existing instance on port 3500
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":3500 "') do (
    taskkill /PID %%a /F >nul 2>&1
)

cd /d C:\users\jeffl\ccprojects\ring\packages\examples

echo Starting Ring Dashboard...
start "" "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe" "http://localhost:3500/dashboard.html"
"C:\Program Files\nodejs\node.exe" --experimental-strip-types web-server.ts
