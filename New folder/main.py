import requests
from decouple import config
import ibm_db


#Connection to the database and API with the utilization of decouple bibliotheque to stock personal informations to .env file
dsn=f"DRIVER={config('DRIVER')};DATABASE={config('DATABASE')};HOSTNAME={config('HOSTNAME')};PORT={config('PORT')};PROTOCOL={config('PROTOCOL')};UID={config('UID')};PWD={config('PWD')}"
conn = ibm_db.connect(dsn,"","")
authorization_key = config('authorization_key')
SUBDOMAIN = config('SUBDOMAIN')

headers = {
    'Authorization': f"Basic {authorization_key}",
    'Accept' : 'application/json',
    "content-type": "application/json"
}
if conn:
    print("connected to ibm_db2")
    shema_name = "TMWIN"
    
    """
    request without duplicate that allow to retrieve most recent commission rate employees based on END_DATE in the table RPT_COMM_RATES_ELI_BHR 
     which is the table where the data in the table RPT_COMM_RATES_ELI are imported
    """
    select_sql = f""" SELECT  BHR_ID, TYPE, CLASS, SITE_NAME, USER_ID, RATE, MULTIPLIER, EFFECTIVE, POOL
    from {shema_name}.RPT_COMM_RATES_ELI_BHR where BHR_ID=467 AND END_DATE='2030-01-01' ORDER BY EFFECTIVE DESC """
    #Executing the sql
    stmt = ibm_db.exec_immediate(conn, select_sql)
    rows = []
    #Formatting data to table form
    result = ibm_db.fetch_assoc(stmt)
    while result:
        #Anytime the request find result, it is saved in the row (by append) that will allow later the API
        # to record by post data to the specific bamboohr table Customcommission
        rows.append(result)
        #Finally we get the result in the form of table
        result = ibm_db.fetch_assoc(stmt)
    for row in rows:
        print(row['BHR_ID'])
        #Get the user bamboorh id in the table that is used with the api to browse all employee with that id for getting later their user_id
        user_id = row['BHR_ID']
        #url for post for posting later the data
        url = f"https://{SUBDOMAIN}.bamboohr.com/api/gateway.php/{SUBDOMAIN}/v1/employees/{user_id}/tables/customCommission"
        #get data in the table customcommission bamboorh consisting to verify the existing or not of the data
        resp = requests.get(f"https://{SUBDOMAIN}.bamboohr.com/api/gateway.php/{SUBDOMAIN}/v1/employees/{user_id}/tables/customCommission", headers=headers)
        #verifying the existing of the data
        existing_data = resp.json()
        """
        new statement consist to compare the existing data in the bamboorh table CustomCommission
        with our database table RPT_COMM_RATES_ELI_BHR
        
        """
        new_data = {
        "customType1": row["TYPE"],
        "customClass": row["CLASS"],
        "customSite": row["SITE_NAME"],
        "customTMWUserID": row["USER_ID"],
        "customRate": row["RATE"],
        "customMultiplier": row["MULTIPLIER"],
        "customEffectiveDate2": row["EFFECTIVE"].isoformat(),
        "customPooledornon-pooled": row["POOL"]    
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
        "customType1": row["TYPE"],
        "customClass": row["CLASS"],
        "customSite": row["SITE_NAME"],
        "customTMWUserID": row["USER_ID"],
        "customRate": row["RATE"],
        "customMultiplier": row["MULTIPLIER"],
        "customEffectiveDate2": row["EFFECTIVE"].isoformat(),
        "customPooledornon-pooled": row["POOL"]
            }
            #publish the data in CustomCommission
            response = requests.post(url, headers=headers, json=data)
    ibm_db.close(conn)
else:
    print("connection failed")
