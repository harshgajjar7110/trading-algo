@echo off

echo Starting services...
start "Backend" cmd /k "cd web/backend && uvicorn app.main:app --reload --port 8000"
timeout /t 2 >nul
start "Frontend" cmd /k "cd web/frontend && npm run dev"
echo Done!
pause
