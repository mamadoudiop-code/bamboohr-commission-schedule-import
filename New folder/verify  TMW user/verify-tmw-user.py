# Importing necessary libraries and modules
import requests                    # For HTTP requests
from decouple import config        # For retrieving configurations from environment variables or .env files
import ibm_db                      # For IBM DB2 database operations
import re                          # For regular expressions
import difflib                     # For sequence comparison (not used in the visible code)
import smtplib                     # For SMTP (email sending) operations
from email.message import EmailMessage
import os                          # For operating system related operations
from exchangelib import (
    IMPERSONATION,
    OAUTH2,
    Account,
    Configuration,
    HTMLBody,
    Identity,
)
from exchangelib import Message as OutlookMessage
from exchangelib import OAuth2Credentials, Version
from exchangelib.version import EXCHANGE_O365
import time                        # For time-related operations
import sys                         # For interacting with the Python interpreter


# Setting up the database connection string for IBM DB2
dsn=f"DRIVER={config('DRIVER')};DATABASE={config('DATABASE')};HOSTNAME={config('HOSTNAME')};PORT={config('PORT')};PROTOCOL={config('PROTOCOL')};UID={config('UID')};PWD={config('PWD')}"
conn = ibm_db.connect(dsn,"","")    # Establishing the database connection

# Retrieving API and email configuration details from environment variables
authorization_key = config('authorization_key')
SUBDOMAIN = config('SUBDOMAIN')
EMAIL_ADDRESS = config('EMAIL_USER')
EMAIL_PASSWORD = config('EMAIL_SECRET')
SMTP_SERVER = 'smtp-mail.outlook.com' # SMTP server for Outlook
SMTP_PORT = 587                       # SMTP server for Outlook

# Setting up headers for API requests, including authorization
headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "authorization": f"Basic {authorization_key}"
}

# Configuring OAuth2 credentials for email operations
credentials = OAuth2Credentials(
client_id=config('CLIENT_ID'),
client_secret=config('CLIENT_SECRET'),
tenant_id=config('TENANT_ID'),
identity=Identity(primary_smtp_address=config('EMAIL_USER')),)

# Configuring the email account with OAuth2 credentials and Exchange settings
configuration = Configuration(credentials=credentials,auth_type=OAUTH2,server=config('EMAIL_HOST'),version=Version(EXCHANGE_O365),)
account = Account(primary_smtp_address=config('EMAIL_USER'),config=configuration,access_type=IMPERSONATION,autodiscover=False,)

# Function to clean a given name by removing specific characters
def clean_name(name):
    # Using regular expression to substitute characters like '!', '?' and '-' with an empty string in 'name'
    # This effectively removes these characters from the name
    return re.sub(r'[!?-]', '', name)

# Function to simulate a loading process for a specified duration
def loading(duration=5):
    # Calculate the end time for the loading process based on the current time and the specified duration
    end_time = time.time()+ duration
    
    # Loop until the current time is less than the calculated end time
    while time.time() < end_time:
        
        # This creates an effect of dynamically updating the same line in the console
        print("Connexion BD...", end="\r")
        # Pause for 0.2 seconds
        time.sleep(0.2)
        # Print "Recherche..." in the same way as above
        print("Recherche...", end="\r")
        time.sleep(0.2)
        # Print "Recherche..." in the same way as above
        print("chargement...", end="\r")
        time.sleep(0.2)

# Check if the database connection 'conn' is established
if conn:
    # Call the loading function to display a loading simulation
    loading()
    
    # Define the schema name to be used in the SQL query
    shema_name = "TMWIN"
    
    # Construct a SQL query to select all records from the USERS table in the specified schema
    select_sql = f"SELECT * from {shema_name}.USERS"
    
    # Execute the SQL query and store the result in 'stmt'
    stmt = ibm_db.exec_immediate(conn, select_sql)
    
    # Initialize lists to store various data
    rows = []   # For storing rows fetched from the database
    row_existing_TMW_user_table_bamboohr = []    # For storing user IDs from BambooHR data
    row_existing_TMW_user_table_user = []        # For storing user IDs from the database
    employee_id_existing = []                   # For storing employee IDs from BambooHR data
    
    # Fetch the first row from the query result
    result = ibm_db.fetch_assoc(stmt)
    
    # Loop to fetch all rows from the query result and append them to 'rows'
    while result:
        rows.append(result)
        result = ibm_db.fetch_assoc(stmt)
    
    # Make an API request to BambooHR to get employee directory data
    response = requests.get(f"https://api.bamboohr.com/api/gateway.php/{SUBDOMAIN}/v1/employees/directory", headers=headers)
    employee = response.json()['employees']
    
    # Iterate over each employee in the BambooHR directory
    for employees in employee: 
        employee_id = employees['id']
        
        # Make an API request to BambooHR to get custom commission data for each employee
        resp = requests.get(f"https://{SUBDOMAIN}.bamboohr.com/api/gateway.php/{SUBDOMAIN}/v1/employees/{employee_id}/tables/customCommission", headers=headers)
        existing_data = resp.json() 
        
        # Collect TMW User IDs and employee IDs from the BambooHR data
        for data in existing_data:
            row_existing_TMW_user_table_bamboohr.append(data['customTMWUserID'])
            employee_id_existing.append(data['employeeId'])
            
    # Collect USER_IDs from the database rows
    for r in rows:
        row_existing_TMW_user_table_user.append(r['USER_ID'])
           
    # Compare and find differences between BambooHR data and database data
    # Find entries that exist in BambooHR but not in the database user table
    diff1 = set(row_existing_TMW_user_table_bamboohr) - set(row_existing_TMW_user_table_user)
    #entries that exist in user table and not in bamboorh
    #diff2 = set(row_existing_TMW_user_table_user) - set(row_existing_TMW_user_table_bamboohr)
    
    # Print the differences
    print("Les entrées suivantes de la table commission schedule n'existent pas dans user", diff1)
    #print("Les entrées suivantes de la table user n'existent pas dans bamboorh", diff2)
    
    # Prepare a list of unique employee IDs
    row_find_out = [] # List for future processing
    liste_employeeid = employee_id_existing
    liste_employeeid = list(set(liste_employeeid))  # Remove duplicates from the list
    
    # Iterate over each employee ID in the list
    for employee_id in liste_employeeid:
        # Print the employee ID for debugging or logging
        employee_id = employee_id
        print(employee_id)
        
        # Make an API request to BambooHR to get custom commission data for the employee
        resp = requests.get(f"https://{SUBDOMAIN}.bamboohr.com/api/gateway.php/{SUBDOMAIN}/v1/employees/{employee_id}/tables/customCommission", headers=headers)
        existing = resp.json()
        print("existing",existing)
        
        # Check if any of the fetched data has a TMW UserID that matches those not found in the database
        for existing in existing:
            if existing['customTMWUserID'] in diff1:
                print("la valeur inexistante dans la base de donnée est: ",existing['customTMWUserID'] ,"noté chez l'employé", existing['employeeId'])
                
                # Fetch the name of the employee with the missing TMW UserID
                get_name = requests.get(f"https://api.bamboohr.com/api/gateway.php/shipenergy/v1/employees/{existing['employeeId']}/?fields=firstName%2ClastName", headers=headers)
                name = get_name.json()['firstName']+" "+get_name.json()['lastName']
                
                # Append a detailed record about the discrepancy to the list
                row_find_out.append(["Nom de l'employé/Employee name:", name,
                                     "Type:", existing['customType1'],
                                     "Class:", existing['customClass'],
                                     "Site:",existing['customSite'],
                                     "TMW UserID:", existing['customTMWUserID'],
                                     "Rate:", existing['customRate'],
                                     "Multiplier:", existing['customMultiplier'],
                                     "Effective Date:", existing['customEffectiveDate2'],
                                     "Pooled or non-pooled :", existing['customPooledornon-pooled'],
                                     "Employee ID",existing['employeeId']
                                     ]
                                    )
                
    
    #Prepare the HTML body of the email with a report of discrepancies
    corps_email = "<html><body><p><i><strong>Version française – English below</strong></i></p><p>Une ou plusieurs entrées dans la table Commission Schedule de BambooHR a un TMW UserID invalide.</p><p>Veuillez vérifier le TMW UserID et faire la correction dans BambooHR.</p><p>Pour toutes questions, veuillez contacter <a href=mailto:integrationteam@shipenergy.com>integrationteam@shipenergy.com</a>. <span style='color:red;'><strong>NE PAS RÉPONDRE À CE COURRIEL.</strong></span></p><p>Merci</p>"
    
    # For each discrepancy, add detailed information to the email body
    i = 0
    ii= 0
    for rowf in row_find_out:
        i+=1
        corps_email += f"<u><p><strong>Entrée {i}</strong></p></u>\n"
        # Formatting the details of each discrepancy
        corps_email += f"{rowf[0][0:16]}: {rowf[1]}<br><br>"
        corps_email += f"<dl>"
        # ... other formatting details ...
        corps_email += f"<dt>{rowf[2]} {rowf[3]}</dt>"
        corps_email += f"<dt> {rowf[4]} {rowf[5]}</dt>"
        corps_email += f"<dt>{rowf[6]}  {rowf[7]}</dt>"
        corps_email += f"<dt> {rowf[8]} {rowf[9]}</dt>"
        corps_email += f"<dt> {rowf[10]} {rowf[11]}</dt>"
        corps_email += f"<dt> {rowf[12]} {rowf[13]}</dt>"
        corps_email += f"<dt> {rowf[14]} {rowf[15]}</dt>"
        corps_email += f"<dt> {rowf[16]}  {rowf[17]}</dt>"
        corps_email += f"<dt>{rowf[18]} {rowf[19]}</dt>"
        corps_email+= "</dl>"
    # Add the English version of the message
    corps_email += "<br><p><i><strong>English version</strong></i></p><p>One or more entries in the BambooHR Commission Schedule table have an invalid TMW UserID. </p><p>Please verify the TMW UserID and make the correction in BambooHR.</p><p>For any questions, please contact <a href=mailto:integrationteam@shipenergy.com>integrationteam@shipenergy.com</a>. <span style='color:red;'><strong>DO NOT REPLY TO THIS EMAIL.</strong></span></p><p>Thank you</p>"
    for rowf in row_find_out:
        ii+=1
        corps_email += f"<u><p><strong>Entry {ii}</strong></p></u>\n"
        corps_email += f"{rowf[0][17:]} {rowf[1]}<br><br>"
        corps_email += f"<dl>"
        corps_email += f"<dt>{rowf[2]} {rowf[3]}</dt>"
        corps_email += f"<dt> {rowf[4]} {rowf[5]}</dt>"
        corps_email += f"<dt>{rowf[6]}  {rowf[7]}</dt>"
        corps_email += f"<dt> {rowf[8]} {rowf[9]}</dt>"
        corps_email += f"<dt> {rowf[10]} {rowf[11]}</dt>"
        corps_email += f"<dt> {rowf[12]} {rowf[13]}</dt>"
        corps_email += f"<dt> {rowf[14]} {rowf[15]}</dt>"
        corps_email += f"<dt> {rowf[16]}  {rowf[17]}</dt>"
        corps_email += f"<dt>{rowf[18]} {rowf[19]}</dt>"
        corps_email+= "</dl></body></html>"
    
    # If there are discrepancies, send an email with the report
    if row_find_out !=[]:
        message = OutlookMessage(
        account=account,
        subject=" COMMISSION | IMPORTANT  | ACTION REQUISE | ACTION REQUIRED",
        body=HTMLBody(corps_email),
        to_recipients=[config("RECIPIENT")])
        message.send()
    else:
        # If everything is correct, print a confirmation message
        print("everything correct")
    
    
        
        
    
    
            