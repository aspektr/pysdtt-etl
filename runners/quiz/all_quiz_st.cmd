@echo off
cd ..\..
echo "Start ETL process from quiz.qf_application to quiz.st_application in multi mode"
C:\ProgramData\Anaconda3\python.exe etl.py --conf configs/quiz.yaml --from qf_application --to st_application --mode multi

IF %ERRORLEVEL% EQU 0 (
    echo "Start ETL process from quiz.qf_form to quiz.st_form in multi mode"
    C:\ProgramData\Anaconda3\python.exe etl.py --conf configs/quiz.yaml --from qf_form --to st_form --mode multi
)

IF %ERRORLEVEL% EQU 0 (
    echo "Start ETL process from quiz.qf_line to quiz.st_line in multi mode"
    C:\ProgramData\Anaconda3\python.exe etl.py --conf configs/quiz.yaml --from qf_line --to st_line --mode multi
)


IF %ERRORLEVEL% EQU 0 (
    echo "Start ETL process from quiz.qf_status to quiz.st_status in multi mode"
    C:\ProgramData\Anaconda3\python.exe etl.py --conf configs/quiz.yaml --from qf_status --to st_status --mode multi
)

IF %ERRORLEVEL% EQU 0 (
echo "Start ETL process from quiz.wp_note to quiz.st_note in multi mode"
C:\ProgramData\Anaconda3\python.exe etl.py --conf configs/quiz.yaml --from wp_note --to st_note --mode multi
)

IF %ERRORLEVEL% EQU 0 (
    echo "Start ETL process from quiz.wp_employee to quiz.st_employee in multi mode"
    C:\ProgramData\Anaconda3\python.exe etl.py --conf configs/quiz.yaml --from wp_employee --to st_employee --mode multi
)

IF %ERRORLEVEL% EQU 0 (
echo "Start ETL process from quiz.wp_assign to quiz.st_assign in multi mode"
C:\ProgramData\Anaconda3\python.exe etl.py --conf configs/quiz.yaml --from wp_assign --to st_assign --mode multi
)

IF %ERRORLEVEL% NEQ 0 echo "ETL failed :("