# Import necessary libraries
import ibm_db
from decouple import config
import requests
import re
import ibm_db_dbi

# Define database connection parameters using environment variables
dsn=f"DRIVER={config('DRIVER')};DATABASE={config('DATABASE')};HOSTNAME={config('HOSTNAME')};PORT={config('PORT')};PROTOCOL={config('PROTOCOL')};UID={config('UID')};PWD={config('PWD')};AUTHENTICATION=SERVER;"
conn = ibm_db.connect(dsn,"","")
conn_dbi = ibm_db_dbi.Connection(conn)
# Get authorization key and subdomain from environment variables
authorization_key = config('authorization_key')
SUBDOMAIN = config('SUBDOMAIN')

# Define headers for API requests
headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "authorization": f"Basic {authorization_key}"
}


# Function to clean the name by removing undesirable characters
def clean_name(name):
    """
    Cleans the given name by removing undesirable characters.

    Args:
        name (str): The input name to be cleaned.

    Returns:
        str: The cleaned name with undesirable characters removed.
    """
    return re.sub(r'[!?-]', '', name)

# Check if the database connection is successful
if conn:
    print("connected to db2")
    shema_name = "TMWIN"
    
     # SQL query to insert data from RPT_COMM_RATES_ELI to RPT_COMM_RATES_ELI_BHR
    """
    Request that consist to insert data from RPT_COMM_RATES_ELI to RPT_COMM_RATES_ELI_BHR. 
    The request take the most updated data and insert it to the table
    """
    select_sql = f"""
    INSERT INTO {shema_name}.RPT_COMM_RATES_ELI_BHR(EFFECTIVE,TYPE,CLASS,SITE_NAME,USER_ID,RATE,MULTIPLIER,
    MULTIPLIER_EFFECTIVE,POOL,END_DATE,INS_TIMESTAMP )
    SELECT EFFECTIVE,TYPE,CLASS,SITE_NAME,USER_ID,RATE,MULTIPLIER,
    MULTIPLIER_EFFECTIVE,POOL,END_DATE,INS_TIMESTAMP
    FROM {shema_name}.RPT_COMM_RATES_ELI source
    WHERE NOT EXISTS (
    SELECT 1 
    FROM {shema_name}.RPT_COMM_RATES_ELI_BHR target
    WHERE (source.EFFECTIVE = target.EFFECTIVE) and (source.TYPE = target.TYPE) and (source.CLASS = target.CLASS) 
    and (source.SITE_NAME = target.SITE_NAME) and (source.USER_ID = target.USER_ID)
    )
    """
    stmt = ibm_db.exec_immediate(conn, select_sql)
    affected_rows = ibm_db.num_rows(stmt)
    
    # Provide information about the number of records affected by the insertion
    #if affected_rows == 0:
        #print(f"{affected_rows} record done, nothing to add; data are updated")
    if affected_rows == 0:
        print(f"{affected_rows} record done")
        # Constructing the SQL query to select distinct BHR_ID and USER_ID
        # from a specified table in the database schema.
        # The query filters to only include rows where BHR_ID is NULL.
        select_sql = f"SELECT DISTINCT BHR_ID,USER_ID from {shema_name}.RPT_COMM_RATES_ELI_BHR where BHR_ID IS NULL"
        
        # Executing the SQL query using the IBM DB2 database connection.
        # The result of the query execution is stored in 'stmt'.
        stmt = ibm_db.exec_immediate(conn, select_sql)
        
        # Initializing an empty list to store the rows fetched from the database.
        rows = []
        
        # Fetching the first row of the query result as a dictionary.
        result = ibm_db.fetch_assoc(stmt)
        
        # Looping through the result set.
        while result:
            # Comment explaining the purpose of this code block:
            # The loop continues as long as there are rows in the result set.
            # Each row found by the query is appended to the 'rows' list.
            # This list can be used later, for example, to post data to a specific
            # table in the BambooHR system (in this context, the 'Customcommission' table).
            rows.append(result)
            
            # Fetching the next row from the result set for the next iteration.
            result = ibm_db.fetch_assoc(stmt)
         
         # Iterate over each row in the 'rows' list   
        for row in rows:
            
         # Extract the USER_ID from the current row for later use in API queries
            user_id = row['USER_ID']
            
            # Send a GET request to the BambooHR API to retrieve the employee directory
            # Note: This assumes that the API only returns active employees and not inactive ones
            response = requests.get(f"https://api.bamboohr.com/api/gateway.php/{SUBDOMAIN}/v1/employees/directory", headers=headers)
            
            # Parse the JSON response to get a list of employee entries
            entry_id = response.json()['employees']
            
            # Iterate over each employee entry in the BambooHR directory
            for entry_id in entry_id:
                
                # Generate a name combination by taking the first letter of the first name and appending the last name
                # and then clean and format the name
                names1 = clean_name(entry_id["firstName"][0] + entry_id["lastName"]).upper().replace("'","")
                
                # Generate another name combination by taking the first name and the first letter of the last name
                # and then clean and format the name
                names2=clean_name(entry_id["firstName"] + entry_id["lastName"][0]).upper().replace("'","")
                
                # Extract the last name of the employee, clean and format it for comparison
                last_name=entry_id["lastName"].upper().replace("'","")
            
                # Update the BHR_ID in the RPT_COMM_RATES_ELI_BHR table if the generated name (names1)
                # matches the USER_ID from the table and BHR_ID is NULL
                if names1 == user_id:
                    update_sql = f"""UPDATE {shema_name}.RPT_COMM_RATES_ELI_BHR
                     SET BHR_ID = {entry_id["id"]}
                     WHERE USER_ID = '{user_id}' AND BHR_ID IS NULL"""
                    ibm_db.exec_immediate(conn, update_sql) 
                
                # Perform similar update as above but consider the scenario where spaces are removed from names1
                if names1.replace(" ","") == user_id:
                    update_sql = f"""UPDATE {shema_name}.RPT_COMM_RATES_ELI_BHR
                     SET BHR_ID = {entry_id["id"]}
                     WHERE USER_ID = '{user_id}' AND BHR_ID IS NULL"""
                    ibm_db.exec_immediate(conn, update_sql) 
                    
                # Update the BHR_ID if the second generated name (names2) matches the USER_ID
                if names2 == user_id:
                    update_sql = f"""UPDATE {shema_name}.RPT_COMM_RATES_ELI_BHR
                     SET BHR_ID = {entry_id["id"]}
                     WHERE USER_ID = '{user_id}' AND BHR_ID IS NULL"""
                    ibm_db.exec_immediate(conn, update_sql) 
                
            
                # Check if the first generated name (names1) does not match the USER_ID
                if names1 != user_id:
                    # Check if the last name of the employee is not a part of the USER_ID
                    if last_name not in user_id:
                        
                        # Split the last name by space and check if the last part of the split name is in USER_ID
                        # This is to handle cases where the last name might have multiple parts
                        if last_name.split()[-1] in user_id:
                            update_sql = f"""UPDATE {shema_name}.RPT_COMM_RATES_ELI_BHR
                            SET BHR_ID = {entry_id["id"]}
                            WHERE USER_ID = '{user_id}' AND BHR_ID IS NULL"""
                            ibm_db.exec_immediate(conn, update_sql) 
                        
                        # Split the last name by dash and check if the last part of the split name is in USER_ID
                        # This handles cases where the last name might be hyphenated
                        if last_name.split('-')[-1] in user_id:
                            update_sql = f"""UPDATE {shema_name}.RPT_COMM_RATES_ELI_BHR
                            SET BHR_ID = {entry_id["id"]}
                            WHERE USER_ID = '{user_id}' AND BHR_ID IS NULL"""
                            ibm_db.exec_immediate(conn, update_sql)
                    
                    # Check if the last name matches the USER_ID starting from the second character
                    # This might handle cases where the USER_ID has a prefix or a single-character error at the start
                    if last_name == user_id[1:]:
                        # Update the BHR_ID in the database for this condition
                        update_sql = f"""UPDATE {shema_name}.RPT_COMM_RATES_ELI_BHR
                        SET BHR_ID = {entry_id["id"]}
                        WHERE USER_ID = '{user_id}' AND BHR_ID IS NULL"""
                        ibm_db.exec_immediate(conn, update_sql)
                    
                    # Check if the last name matches the USER_ID starting from the third character
                    # This handles cases similar to the above but with a two-character discrepancy at the start
                    if last_name == user_id[2:]:
                        # Update the BHR_ID in the database for this condition
                        update_sql = f"""UPDATE {shema_name}.RPT_COMM_RATES_ELI_BHR
                        SET BHR_ID = {entry_id["id"]}
                        WHERE USER_ID = '{user_id}' AND BHR_ID IS NULL"""
                        ibm_db.exec_immediate(conn, update_sql)

        #Define a SQL query to select various fields from a specific table in the database schema.
        # The query filters orders the results by EFFECTIVE date in descending order.        
        select_sql2 = f""" SELECT  BHR_ID, TYPE, CLASS, SITE_NAME, USER_ID, RATE, MULTIPLIER, EFFECTIVE, POOL
                    from {shema_name}.RPT_COMM_RATES_ELI_BHR where END_DATE='2030-01-01' ORDER BY EFFECTIVE DESC """
        
        # Execute the SQL query using the IBM DB2 database connection.
        stmt2 = ibm_db.exec_immediate(conn, select_sql2)
        
        # Initialize an empty list to store rows fetched from the database.
        rows2 = []
        
        # Fetch the first row from the query result and store it in a variable.
        result2 = ibm_db.fetch_assoc(stmt2)
        
        # Iterate over the query results and append each row to the 'rows2' list.
        while result2:
        #Anytime the request find result, it is saved in the row (by append) that will allow later the API
        # to record by post data to the specific bamboohr table Customcommission
            rows2.append(result2)
        #Finally we get the result in the form of table
            result2 = ibm_db.fetch_assoc(stmt2)
        
        # Iterate over the query results and append each row to the 'rows2' list.
        for rowp in rows2:
            # Print the BHR_ID of each row.
            print(rowp['BHR_ID'])
            
             # Extract the BHR_ID from the current row for later use in API requests.
            user_id = rowp['BHR_ID']
            
            if user_id is None:
                print("impossible")
            else:
            # Define the URL for a BambooHR API endpoint for a specific employee's customCommission table.
                url = f"https://{SUBDOMAIN}.bamboohr.com/api/gateway.php/{SUBDOMAIN}/v1/employees/{user_id}/tables/customCommission"
            
            # Send a GET request to the BambooHR API to retrieve data from the customCommission table for the specified employee.
                resp = requests.get(f"https://{SUBDOMAIN}.bamboohr.com/api/gateway.php/{SUBDOMAIN}/v1/employees/{user_id}/tables/customCommission", headers=headers)
            
            # Parse the JSON response to get existing data.
                existing_data = resp.json()

            # Define new data to potentially be added to BambooHR based on the current row.
                new_data = {
            "customType1": rowp["TYPE"],
            "customClass": rowp["CLASS"],
            "customSite": rowp["SITE_NAME"],
            "customTMWUserID": rowp["USER_ID"],
            "customRate": rowp["RATE"],
            "customMultiplier": rowp["MULTIPLIER"],
            "customEffectiveDate2": rowp["EFFECTIVE"].isoformat(),
            "customPooledornon-pooled": rowp["POOL"]    
                }
            
            # Initialize a variable to track if the new data already exists in BambooHR.
                data_exists = False
            
            # Check if the new data already exists in the existing data from BambooHR.
                for data in existing_data:
            #Comparing the element in the data
                    if data["customTMWUserID"] == new_data['customTMWUserID'] and data['customClass'] == new_data['customClass'] and data['customSite'] == new_data['customSite'] and data['customClass'] == new_data['customClass'] and data["customEffectiveDate2"] == new_data['customEffectiveDate2'] :
                        print("Data still exist in bamboorh table")
                        data_exists = True
                        break
                
            # If the new data does not exist, post it to the BambooHR API.
                if not data_exists:
                # Send a POST request to BambooHR to add the new data.
                    data = {
            "customType1": rowp["TYPE"],
            "customClass": rowp["CLASS"],
            "customSite": rowp["SITE_NAME"],
            "customTMWUserID": rowp["USER_ID"],
            "customRate": rowp["RATE"],
            "customMultiplier": rowp["MULTIPLIER"],
            "customEffectiveDate2": rowp["EFFECTIVE"].isoformat(),
            "customPooledornon-pooled": rowp["POOL"]
                }
            #publish the data in CustomCommission
                response = requests.post(url, headers=headers, json=data)
    ibm_db.close(conn)
else:
    print("connection failed")