@echo off
chcp 65001 >nul
echo ======================================
echo   Avito News Parser
echo ======================================
echo.
echo [1/2] Проверка обновлений...
for /f "delims=" %%i in ('git rev-parse HEAD') do set OLD=%%i
git pull --quiet
for /f "delims=" %%i in ('git rev-parse HEAD') do set NEW=%%i
if "%OLD%"=="%NEW%" (
    echo   Всё актуально.
) else (
    echo.
    echo   Новые изменения:
    git log %OLD%..%NEW% --no-merges --pretty=format:"    - %%s"
    echo.
)
echo.
echo [2/2] Запуск программы...
echo.
python main.py
pause
