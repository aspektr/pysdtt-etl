echo "Start ETL process from quiz.qf_application to quiz.st_application in multi mode"
cd ..\..
C:\ProgramData\Anaconda3\python.exe etl.py --conf configs/quiz.yaml --from qf_application --to st_application --mode multi
echo Exit Code is %errorlevel%