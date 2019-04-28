echo "Start ETL process from quiz.qf_status to quiz.st_status in multi mode"
cd ..\..
C:\ProgramData\Anaconda3\python.exe etl.py --conf configs/quiz.yaml --from qf_status --to st_status --mode multi
echo Exit Code is %errorlevel%