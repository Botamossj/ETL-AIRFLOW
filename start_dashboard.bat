@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo Iniciando Dashboard de Contratos...
echo.
python run_dashboard.py
pause


