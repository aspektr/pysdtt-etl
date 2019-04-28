echo "Start ETL process from quiz.wp_assign to quiz.st_assign in multi mode"
cd ..\..
C:\ProgramData\Anaconda3\python.exe etl.py --conf configs/quiz.yaml --from wp_assign --to st_assign --mode multi
echo Exit Code is %errorlevel%