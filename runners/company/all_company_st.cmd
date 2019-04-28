@echo off
cd ../..
echo "Start ETL process from collection company to st_company in multi mode"
C:\ProgramData\Anaconda3\python.exe etl.py --conf configs/company.yaml --from ms_company --to st_company --mode multi

IF %ERRORLEVEL% EQU 0 (
    echo "Start ETL process from collection person to st_person in multi mode"
    C:\ProgramData\Anaconda3\python.exe etl.py --conf configs/company.yaml --from person --to st_person --mode multi
)

IF %ERRORLEVEL% NEQ 0 echo "ETL failed :("