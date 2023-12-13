# BambooHR Commission Schedule Import


## Name
Python Script for Database and API Interaction

## Description
This Python script is designed to interact with an IBM DB2 database and the BambooHR API. It performs data insertion from one table to another in the database and then updates the BambooHR system based on the database records.

## Key Features 

1. Database Connection:

    - Establishes a connection to an IBM DB2 database using credentials and parameters defined in environment variables.

2. Data Insertion:

    - Performs an SQL operation to insert data from one table (RPT_COMM_RATES_ELI) to another (RPT_COMM_RATES_ELI_BHR) in the database.

3.  API Interaction:

    - Makes requests to the BambooHR API to retrieve and update employee information based on the database records.

4. Data Processing:

    - Processes data fetched from the database and formats it for updating in BambooHR. Checks if the data to be updated already exists in BambooHR and updates it only if necessary.

5.  Database and API Integration:

    - The script integrates database records with the BambooHR system, ensuring that the data in BambooHR is up-to-date with the database.

6.  Environment Variable Management: 

    - Uses the decouple library for secure access to configuration data.

## Requirement
- Python
- IBM DB2 database
- Access to BambooHR API
- Environment variables for database and API configurations

## Dependencies 

- `python-decouple`: For managing environment variables.
- `requests`: For making API requests.
- `ibm_db`: For connecting to the IBM Db2 database.
- `re`: For regular expressions in Python.

## Docker Install
**Clone Repository**
1. `git clone https://git.shipenergy.com/etg-integrations/commission/bamboohr-commission-schedule-import.git`

**Moved into bamboohr-commission-schedule-import.git DIR**

2. `$ cd bamboohr-commission-schedule-import`

**Build Docker Container**

3. `docker build -t bamboohr-commission-schedule-import .`


To run the docker container manually you can run the following command:
`docker run bamboohr-commission-schedule-import`

## Configuration and Setup

- Set up environment variables for database credentials, API keys.
- Install necessary Python libraries as mentioned in the dependencies.

## Installation
```
Pip install -r requirement.txt

```
Edit the .env file
-  Provide the database credentials, Database name, your UID and password
-  Provide an authorization bamboorh key. To get this authorization key you must follow these steps by:
    - getting firstly an API key from bamboorh
    - Go to bamboorh API documentation (https://documentation.bamboohr.com/reference/get-employee), provide your API and Password and get the authorization key in the headers

## How the Scripts Works

1. Database Connection: Establishes a connection to an IBM DB2 database using credentials specified in environment variables.

2. Data Fetching and Insertion: Executes SQL queries to fetch data from the database and potentially insert or update records.

3. API Requests: Sends requests to the BambooHR API to retrieve employee information and compare it with database records.


## Usage

1. Ensure all necessary environment variables are set.

2. The first script main.py will automatically connect to the database, import data from RPT_COMM_ELI to RPT_COMM_ELI_BHR, Update the BHR_ID, and publish data to bamboorh if tis is not  existed in bamboohr


## Notes

- The script assumes active database and BambooHR API connections.
- Error handling for database and API interactions is essential for production use.
- The script prints log messages for successful operations and potential discrepancies


## Contributing
open to contributions 

## Authors and acknowledgment
- Mamadou Bamba Diop: Integration Specialist


## Project status

