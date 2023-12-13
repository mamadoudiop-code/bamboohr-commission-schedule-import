# Import necessary libraries
import ibm_db
from decouple import config
import requests
import re
import ibm_db_dbi

# Define database connection parameters using environment variables
dsn=f"DRIVER={config('DRIVER')};DATABASE={config('DATABASE')};HOSTNAME={config('HOSTNAME')};PORT={config('PORT')};PROTOCOL={config('PROTOCOL')};UID={config('UID')};PWD={config('PWD')};AUTHENTICATION=SERVER;"
conn = ibm_db.connect(dsn,"","")

if conn:
    print("ok")
else:
    print("no")