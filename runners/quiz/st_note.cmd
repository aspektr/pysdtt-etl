echo "Start ETL process from quiz.wp_note to quiz.st_note in multi mode"
cd ..\..
C:\ProgramData\Anaconda3\python.exe etl.py --conf configs/quiz.yaml --from wp_note --to st_note --mode multi
echo Exit Code is %errorlevel%