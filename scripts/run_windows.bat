@echo off
setlocal

cd /d "%~dp0.."

if not exist ".venv\\Scripts\\python.exe" (
  echo Venv not found. Run scripts\\install_windows.bat first.
  exit /b 1
)

call ".venv\\Scripts\\activate.bat"
python main.py %*
