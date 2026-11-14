@echo off
chcp 65001 >nul
echo Cerrando servidor Visor WRF...
taskkill /F /FI "WINDOWTITLE eq Visor WRF*" >nul 2>&1
for /f "tokens=2" %%a in ('netstat -aon ^| findstr :8765 ^| findstr LISTENING') do (
    taskkill /F /PID %%a >nul 2>&1
)
echo Servidor cerrado.
timeout /t 2 >nul
