echo "Start ETL process from quiz.qf_line to quiz.st_line in multi mode"
cd ..\..
C:\ProgramData\Anaconda3\python.exe etl.py --conf configs/quiz.yaml --from qf_line --to st_line --mode multi
echo Exit Code is %errorlevel%