@echo off
chcp 65001 >nul
echo ============================================
echo  Visor WRF Argentina - Modo consola
echo ============================================
echo.
echo Servidor en: http://localhost:8765
echo Presionar Ctrl+C para cerrar.
echo.

REM Verificar Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python no encontrado. Instalar Python 3.x primero.
    pause
    exit /b 1
)

python server.py
