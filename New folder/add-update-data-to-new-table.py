import ibm_db
from decouple import config
import requests
import re

dsn=f"DRIVER={config('DRIVER')};DATABASE={config('DATABASE')};HOSTNAME={config('HOSTNAME')};PORT={config('PORT')};PROTOCOL={config('PROTOCOL')};UID={config('UID')};PWD={config('PWD')}"
conn = ibm_db.connect(dsn,"","")
authorization_key = config('authorization_key')
SUBDOMAIN = config('SUBDOMAIN')

headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "authorization": f"Basic {authorization_key}"
}
#Function consisting to erase indesirable caracter in the name and lastname
def clean_name(name):
    return re.sub(r'[!?-]', '', name)
if conn:
    print("connected to db2")
    shema_name = "TMWIN"
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
    #give the number of record that is done by the request.
    if affected_rows == 0:
        print(f"{affected_rows} record done, nothing to add data are updated")
    if affected_rows != 0:
        print(f"{affected_rows} record done")
        """
    request without duplicate that allow to get user where bamboorh_id field is null after data in 
    RPT_COMM_RATES_ELI are imported in RPT_COMM_RATES_ELI_BHR
    
    """
        select_sql = f"SELECT DISTINCT BHR_ID,USER_ID from {shema_name}.RPT_COMM_RATES_ELI_BHR where BHR_ID IS NULL"
        stmt = ibm_db.exec_immediate(conn, select_sql)
        rows = []
        #Formatting data to table form
        result = ibm_db.fetch_assoc(stmt)
        while result:
        #Anytime the request find result, it is saved in the row (by append) that will allow later the API
        # to record by post data to the specific bamboohr table Customcommission
            rows.append(result)
            result = ibm_db.fetch_assoc(stmt)
        for row in rows:
        #Get user_id for browsing later the API
            user_id = row['USER_ID']
        #It's important to know the API take just account the active employee and not inactive 
            response = requests.get(f"https://api.bamboohr.com/api/gateway.php/{SUBDOMAIN}/v1/employees/directory", headers=headers)
            entry_id = response.json()['employees']
            for entry_id in entry_id:
            #Mapping employee name and lastname considering the first letter of the firstname include the rest of the name
                names1 = clean_name(entry_id["firstName"][0] + entry_id["lastName"]).upper().replace("'","")
            #Mapping employee name and lastname considering the first letter of the lastname include the rest of the firstname
                names2=clean_name(entry_id["firstName"] + entry_id["lastName"][0]).upper().replace("'","")
                #print(names2)
            #initialize a last_name variable in case where the names1 don't match with the TMZ user ID in bamboorh
                last_name=entry_id["lastName"].upper().replace("'","")
            #Updating the BHR_ID when the TMW USER ID match with user id in the table RPT_COMM_RATES_ELI_BHR
                if names1 == user_id:
                    update_sql = f"""UPDATE {shema_name}.RPT_COMM_RATES_ELI_BHR
                     SET BHR_ID = {entry_id["id"]}
                     WHERE USER_ID = '{user_id}' AND BHR_ID IS NULL"""
                    ibm_db.exec_immediate(conn, update_sql) 
                if names1.replace(" ","") == user_id:
                    update_sql = f"""UPDATE {shema_name}.RPT_COMM_RATES_ELI_BHR
                     SET BHR_ID = {entry_id["id"]}
                     WHERE USER_ID = '{user_id}' AND BHR_ID IS NULL"""
                    ibm_db.exec_immediate(conn, update_sql) 
                if names2 == user_id:
                    update_sql = f"""UPDATE {shema_name}.RPT_COMM_RATES_ELI_BHR
                     SET BHR_ID = {entry_id["id"]}
                     WHERE USER_ID = '{user_id}' AND BHR_ID IS NULL"""
                    ibm_db.exec_immediate(conn, update_sql) 
                #print(entry_id["id"])
                #print("ok")
            
                #This next step of verification is a condition set consist to verify if the 2 id aren't matched firsly.
                #If they aren't matched a new step is put in place that consist to verify if last_name exist in The table or not.
                #I have prefered verifying firstly if last_name don't exist in user_id because It has allowed me to determine the main reasons why
                #these hasn't a 100% match. It's due to the presence of many caracter like space, dash and so on. To resolve this problem and allowing a reliable verification
                #the solution was to split if last_name contain space and dash.
                if names1 != user_id:
                    if last_name not in user_id:
                    #parts = last_name.split()
                    #new_last_name = parts[-1]
                    #Splitting the space of the last_name of the user
                        if last_name.split()[-1] in user_id:
                            update_sql = f"""UPDATE {shema_name}.RPT_COMM_RATES_ELI_BHR
                            SET BHR_ID = {entry_id["id"]}
                            WHERE USER_ID = '{user_id}' AND BHR_ID IS NULL"""
                            ibm_db.exec_immediate(conn, update_sql) 
                    #Splitting the dash of the last_name of the user
                        if last_name.split('-')[-1] in user_id:
                            update_sql = f"""UPDATE {shema_name}.RPT_COMM_RATES_ELI_BHR
                            SET BHR_ID = {entry_id["id"]}
                            WHERE USER_ID = '{user_id}' AND BHR_ID IS NULL"""
                            ibm_db.exec_immediate(conn, update_sql)
                    if last_name == user_id[1:]:
                        update_sql = f"""UPDATE {shema_name}.RPT_COMM_RATES_ELI_BHR
                        SET BHR_ID = {entry_id["id"]}
                        WHERE USER_ID = '{user_id}' AND BHR_ID IS NULL"""
                        ibm_db.exec_immediate(conn, update_sql)
                    if last_name == user_id[2:]:
                        update_sql = f"""UPDATE {shema_name}.RPT_COMM_RATES_ELI_BHR
                        SET BHR_ID = {entry_id["id"]}
                        WHERE USER_ID = '{user_id}' AND BHR_ID IS NULL"""
                        ibm_db.exec_immediate(conn, update_sql)
                
                """
            request without duplicate that allow to retrieve most recent commission rate employees based on END_DATE in the table RPT_COMM_RATES_ELI_BHR 
                which is the table where the data in the table RPT_COMM_RATES_ELI are imported
                """
        select_sql2 = f""" SELECT  BHR_ID, TYPE, CLASS, SITE_NAME, USER_ID, RATE, MULTIPLIER, EFFECTIVE, POOL
                    from {shema_name}.RPT_COMM_RATES_ELI_BHR where BHR_ID=467 AND END_DATE='2030-01-01' ORDER BY EFFECTIVE DESC """
                    #Executing the sql
        stmt2 = ibm_db.exec_immediate(conn, select_sql2)
        rows2 = []
        #Formatting data to table form
        result2 = ibm_db.fetch_assoc(stmt2)
        while result2:
        #Anytime the request find result, it is saved in the row (by append) that will allow later the API
        # to record by post data to the specific bamboohr table Customcommission
            rows2.append(result2)
        #Finally we get the result in the form of table
            result2 = ibm_db.fetch_assoc(stmt2)
        for rowp in rows2:
            print(rowp['BHR_ID'])
        #Get the user bamboorh id in the table that is used with the api to browse all employee with that id for getting later their user_id
            user_id = rowp['BHR_ID']
        #url for post for posting later the data
            url = f"https://{SUBDOMAIN}.bamboohr.com/api/gateway.php/{SUBDOMAIN}/v1/employees/{user_id}/tables/customCommission"
        #get data in the table customcommission bamboorh consisting to verify the existing or not of the data
            resp = requests.get(f"https://{SUBDOMAIN}.bamboohr.com/api/gateway.php/{SUBDOMAIN}/v1/employees/{user_id}/tables/customCommission", headers=headers)
        #verifying the existing of the data
            existing_data = resp.json()
       
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
        #Initialise our variable as False
            data_exists = False
            for data in existing_data:
            #Comparing the element in the data
                if data["customTMWUserID"] == new_data['customTMWUserID'] and data['customClass'] == new_data['customClass'] and data['customSite'] == new_data['customSite'] and data['customClass'] == new_data['customClass'] and data["customEffectiveDate2"] == new_data['customEffectiveDate2'] :
                    print("Data still exist in bamboorh table")
                    data_exists = True
                    break
            """
            Publish the data in the bamboorh table CustomCommision if not existed
            """
            if not data_exists:
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