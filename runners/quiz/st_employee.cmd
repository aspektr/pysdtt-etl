echo "Start ETL process from quiz.wp_employee to quiz.st_employee in multi mode"
cd ..\..
C:\ProgramData\Anaconda3\python.exe etl.py --conf configs/quiz.yaml --from wp_employee --to st_employee --mode multi
echo Exit Code is %errorlevel%