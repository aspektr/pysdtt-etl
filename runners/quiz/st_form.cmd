echo "Start ETL process from quiz.qf_form to quiz.st_form in multi mode"
cd ..\..
C:\ProgramData\Anaconda3\python.exe etl.py --conf configs/quiz.yaml --from qf_form --to st_form --mode multi
echo Exit Code is %errorlevel%