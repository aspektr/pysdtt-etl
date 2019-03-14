rem C:\ProgramData\Anaconda3\python.exe etl.py --from quiz_db --to application_history
rem C:\ProgramData\Anaconda3\python.exe etl.py --from test_source --to temp_table
rem C:\ProgramData\Anaconda3\python.exe etl.py
C:\ProgramData\Anaconda3\python.exe etl.py --from test_source2 --to temp_table --mode row-by-row
rem C:\ProgramData\Anaconda3\python.exe etl.py --from test_json --to json_test --mode row-by-row
rem C:\ProgramData\Anaconda3\python.exe etl.py --from quiz_db --to application_history --mode multi