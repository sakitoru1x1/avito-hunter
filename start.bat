@echo off
where py >NUL 2>&1
if %errorlevel%==0 (
    set PY=py
) else (
    set PY=python
)

where git >NUL 2>&1
if %errorlevel%==0 (
    if exist ".git" (
        echo Updating from GitHub...
        git pull --ff-only
        echo.
    )
)

echo Installing dependencies...
%PY% -m pip install -r requirements.txt
if errorlevel 1 (
    echo Trying pip directly...
    pip install -r requirements.txt
)

echo.
echo Starting...
%PY% main.py
pause
