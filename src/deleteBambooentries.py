import requests
from decouple import config
import simplejson as json


authorization_key = config('authorization_key')
SUBDOMAIN = config('SUBDOMAIN')

headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "authorization": f"Basic {authorization_key}"
}

resp = requests.get(f"https://api.bamboohr.com/api/gateway.php/{SUBDOMAIN}/v1/employees/directory", headers=headers)
employee = resp.json()['employees']
    
# Iterate over each employee in the BambooHR directory
for employees in employee: 
    employee_id = employees['id']

    #retrieve all entries
    response = requests.get(f"https://{SUBDOMAIN}.bamboohr.com/api/gateway.php/{SUBDOMAIN}/v1/employees/{employee_id}/tables/customCommission", headers=headers)
    entries = response.json()
    print(entries)
    for entry in entries:
        entry_id = entry['id']
        entry_employee = entry['employeeId']
        print(entry_employee)
        delete_response = requests.delete(f"https://{SUBDOMAIN}.bamboohr.com/api/gateway.php/{SUBDOMAIN}/v1/employees/{entry_employee}/tables/customCommission/{entry_id}", headers=headers)
    
        if delete_response.status_code != 200:
            print("Failed to delete entry")

