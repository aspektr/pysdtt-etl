echo "Start ETL process from collection company to st_company in multi mode"
cd ..\..
C:\ProgramData\Anaconda3\python.exe etl.py --conf configs/company.yaml --from ms_company --to st_company --mode multi
echo Exit Code is %errorlevel%