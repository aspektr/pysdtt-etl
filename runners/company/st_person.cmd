echo "Start ETL process from collection person to st_person in multi mode"
cd ..\..
C:\ProgramData\Anaconda3\python.exe etl.py --conf configs/company.yaml --from person --to st_person --mode multi
echo Exit Code is %errorlevel%