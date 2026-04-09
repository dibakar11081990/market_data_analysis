import os
import json
import requests
import time
import sys
import getopt
import os.path


def create_dbnd_connection(AIRFLOW_URL, TENANT_NAME, HEADERS, DBND_DAG_NAME):
    try:
        print(__file__)
        file = open(os.path.dirname(__file__) + '/../dbnd.txt')
        file_content = file.readlines()
        print("File Content: ", file_content)
        if "failed" in "".join(file_content) or "warning" in "".join(file_content):
            return {"status": "failed", "error": file_content}
        file.close()

        json_data = ''.join(file_content[2:])
        dbnd_token = json.loads(json_data)

        # Remove any existing connection if there
        URL = f'https://deployments.{AIRFLOW_URL}/{TENANT_NAME}/airflow/api/v1/connections/dbnd_config'
        response = requests.delete(url=URL, headers=HEADERS, json={})
        print("Airflow connection deletion response:")
        print(f"Status code: {response.status_code}")

        if response.status_code != 204 and response.status_code != 404:
            return {"status": "failed", "error": f"Error with removing existing connection with the error: {response.text}"}
        
        time.sleep(5)
        
        # Construct data payload for Airflow connection creation
        dbnd_token['airflow_monitor']['dag_ids'] = ""
        dbnd_token['airflow_monitor']['is_sync_enabled'] = "true" if dbnd_token['airflow_monitor']['is_sync_enabled'] is True else "false"
        dbnd_token['tracking']['track_source_code'] = "true" if dbnd_token['tracking']['track_source_code'] is True else "false"
        token = dbnd_token['core']['databand_access_token']
        dbnd_token['core']['databand_access_token'] = ""
        dbnd_token['core'].pop('databand_access_token', None)
        
        json_string = json.dumps(dbnd_token)
        payload = {
            "connection_id": "dbnd_config",
            "conn_type": "http",
            "description": "Connection to automate Databand Airflow Integration",
            "host": "",
            "login": "",
            "schema": "",
            "port": 0,
            "password": token,
            "extra": json_string
        }
        
        data = json.dumps(payload)
        print(f"[DEBUG] {data}")
        URL = f'https://deployments.{AIRFLOW_URL}/{TENANT_NAME}/airflow/api/v1/connections'
        response = requests.post(url=URL, headers=HEADERS, data=data)
        print("Airflow connection creation response:")
        print(f"Status code: {response.status_code}")

        if response.status_code != 200:
            return {"status": "failed", "error": f"Error with creating connection with the error: {response.text}"}
        
        time.sleep(60)

        # Unpause DAG
        URL = f'https://deployments.{AIRFLOW_URL}/{TENANT_NAME}/airflow/api/v1/dags/{DBND_DAG_NAME}'
        response = requests.patch(
            url=URL,
            headers=HEADERS,
            data='''{
                "is_paused": false
            }'''
        )
        print("Unpause DAG response:")
        print(f"Status code: {response.status_code}")

        if response.status_code != 200:
            return {"status": "failed", "error": f"Error with unpausing DAG with the error: {response.text}"}
        
        return {"status": "success"}
    except FileNotFoundError:
        return {"status": "failed", "error": "The file does not exist."}
    except json.JSONDecodeError as e:
        return {"status": "failed", "error": f"Error parsing JSON: {e}"}
    except requests.RequestException as e:
        return {"status": "failed", "error": f"Error creating Airflow connection: {e}"}
    except Exception as e:
        return {"status": "failed", "error": f"An unexpected error occurred: {e}"}
    

def get_args(args):
    try:
        argumentList = args[1:]
        options = "u:t:d:s:"
        long_options = ["URL=", "TENANT=", "DATABAND_DAG=", "SVC_ACC_TOKEN="]
        arguments, values = getopt.getopt(argumentList, options, long_options)
        for currentArgument, currentValue in arguments:
            if currentArgument in ("-u", "--URL"):
                AIRFLOW_URL = ".".join(currentValue.split('.')[1:])
            elif currentArgument in ("-t", "--TENANT"):
                TENANT_NAME = currentValue
            elif currentArgument in ("-d", "--DATABAND_DAG"):
                DBND_DAG_NAME = currentValue
            elif currentArgument in ("-s", "--SVC_ACC_TOKEN"):
                ASTRO_TOKEN = currentValue
        return AIRFLOW_URL, TENANT_NAME, DBND_DAG_NAME, ASTRO_TOKEN
                
    except getopt.error as err:
        print(str(err))
        return 0, 0, 0, 0


def main(AIRFLOW_URL, TENANT_NAME, HEADERS, DBND_DAG_NAME):
    response = create_dbnd_connection(AIRFLOW_URL, TENANT_NAME, HEADERS, DBND_DAG_NAME)
    if response['status'] == "failed":
        print("DBND Integration Failed with the error: ", response['error'])


if __name__ == "__main__":
    AIRFLOW_URL, TENANT_NAME, DBND_DAG_NAME, ASTRO_TOKEN = get_args(sys.argv)
    if AIRFLOW_URL == 0 or TENANT_NAME == 0 or DBND_DAG_NAME == 0 or ASTRO_TOKEN == 0:
        print("Unable to Fetch Args")
        sys.exit(0)

    HEADERS = {
        "Authorization": ASTRO_TOKEN,
        'Accept': 'application/json',
        'Content-type': 'application/json'
    }
    main(AIRFLOW_URL, TENANT_NAME, HEADERS, DBND_DAG_NAME)
