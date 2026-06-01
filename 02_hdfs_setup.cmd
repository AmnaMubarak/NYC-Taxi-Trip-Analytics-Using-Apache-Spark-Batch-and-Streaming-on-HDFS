@echo on
setlocal EnableDelayedExpansion

echo ============================================
echo   HDFS Setup - NYC Taxi Data Upload
echo ============================================

REM Create HDFS directories
echo [1/4] Creating HDFS directories...

call hdfs dfs -mkdir -p /user/taxi/raw
call hdfs dfs -mkdir -p /user/taxi/streaming_input
call hdfs dfs -mkdir -p /user/taxi/streaming_output
call hdfs dfs -mkdir -p /user/taxi/batch_output

REM Data directory
set "DATA_DIR=D:\Osama\Uni\BigData\project\project\project\data\raw"

echo [2/4] Uploading parquet files...

IF NOT EXIST "%DATA_DIR%" (
    echo ERROR: Directory not found:
    echo %DATA_DIR%
    pause
    exit /b 1
)

set /a FILE_COUNT=0

REM Loop through parquet files
for %%F in ("%DATA_DIR%\yellow_tripdata_*.parquet") do (

    if exist "%%F" (

        echo Uploading %%~nxF

        call hdfs dfs -put -f "%%F" /user/taxi/raw/

        if !errorlevel! == 0 (
            set /a FILE_COUNT+=1
        ) else (
            echo Failed to upload %%~nxF
        )
    )
)

set /a FILE_COUNT=0
echo fhvhv_tripdata_*.parquet files...
REM Loop through parquet files
for %%F in ("%DATA_DIR%\fhvhv_tripdata_*.parquet") do (

    if exist "%%F" (

        echo Uploading %%~nxF

        call hdfs dfs -put -f "%%F" /user/taxi/raw/

        if !errorlevel! == 0 (
            set /a FILE_COUNT+=1
        ) else (
            echo Failed to upload %%~nxF
        )
    )
)

echo.
echo Uploaded !FILE_COUNT! files.

echo.
echo [3/4] Verifying upload...
call hdfs dfs -ls /user/taxi/raw/

echo.
echo [4/4] Storage usage...
call hdfs dfs -du -h /user/taxi/raw/

echo.
echo ============================================
echo Completed
echo ============================================

pause