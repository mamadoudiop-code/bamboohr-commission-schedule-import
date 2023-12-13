import requests
import pandas as pd 
from decouple import config

authorization_key = config('authorization_key')
SUBDOMAIN = config('SUBDOMAIN')

headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "authorization": f"Basic {authorization_key}"
}

response = requests.get(f"https://api.bamboohr.com/api/gateway.php/{SUBDOMAIN}/v1/employees/directory", headers=headers)
employee = response.json()['employees']
row = []
for employees in employee: 
        employee_id = employees['id']
        resp = requests.get(f"https://{SUBDOMAIN}.bamboohr.com/api/gateway.php/{SUBDOMAIN}/v1/employees/{employee_id}/tables/customCommission", headers=headers)
        existing_data = resp.json() 
        if existing_data != []:
            for exist_data in existing_data:
                get_name = requests.get(f"https://api.bamboohr.com/api/gateway.php/shipenergy/v1/employees/{exist_data['employeeId']}/?fields=firstName%2ClastName", headers=headers)
                #name = get_name.json()['firstName']+" "+get_name.json()['lastName']
                print(exist_data)
                row.append([get_name.json()['firstName'],
                            get_name.json()['lastName'],
                            exist_data['employeeId'], 
                            exist_data['customTMWUserID'],
                            exist_data['customType1'],
                            exist_data['customClass'],
                            exist_data['customSite'],
                            exist_data['customRate'],
                            exist_data['customMultiplier'],
                            exist_data['customPooledornon-pooled'],
                            exist_data['customEffectiveDate2'],
                            ])

df = pd.DataFrame(row, columns=['firstname', 'Lastname','EmployeeID', 'TMWUserID', 'Type', 'Class', 'Site', 'Rate','Multiplier', 'Pooledornon-pooled', 'effectiveDate'])
df.to_excel("customcommissiondata.xlsx", index=False)