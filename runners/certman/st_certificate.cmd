echo "Start ETL process from ms certman to st_sertificate in multi mode"
cd ../..
C:\ProgramData\Anaconda3\python.exe etl.py --conf configs/certman.yaml --from ms_certman --to st_certificate --mode multi
echo Exit Code is %errorlevel%
