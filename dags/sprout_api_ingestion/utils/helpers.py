import snowflake.connector
import os
from dags.common.py.utils.verbose_log import verbose, log
from dags.common.py.utils.utility_functions import aws_conn_credentials
from datetime import datetime, timedelta
from airflow.models import Variable
import json
import runpy
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine,text
import shutil
from airflow.hooks.base_hook import BaseHook 
import pytz
from tqdm import tqdm
from multiprocessing import context
import requests
from datetime import datetime
from dags.common.py.utils.utility_functions import get_secretmanager_parameters







ENVIRONMENT = 'dev' if os.environ['ENVIRONMENT'].lower() == 'stg' else os.environ['ENVIRONMENT']

SNOWFLAKE_CONNECTION_ID = os.environ['SNOWFLAKE_CONN']
snowflake_connection = BaseHook.get_connection(SNOWFLAKE_CONNECTION_ID)

aws_access_key, aws_secret_key = aws_conn_credentials()

################################################################################################################





def generate_dbt_command(**kwargs):          # FIX 1: *args removed; use **kwargs only
    dbt_target  = ENVIRONMENT.lower()
    project_dir = '/usr/local/airflow/dbt/mars_dbt/'
    dbt_dir     = '/usr/local/airflow/dbt_venv/bin/dbt'

    # FIX 1 & 2: access via kwargs, and key corrected to 'dbt_variable_template'
    operation            = kwargs['operation']
    model                = kwargs['model']
    profile              = kwargs['profile']
    dbt_variable_template = kwargs['dbt_variable_template']   # was: args['dbt_variable']

    dbt_command = (
        f"{dbt_dir} {operation} --vars '{dbt_variable_template}' "
        f"-s {model} --project-dir {project_dir} "
        f"--profiles-dir {project_dir} --profile {profile} --target {dbt_target} "
        "--log-path /tmp --target-path /tmp/dbt_target"
    )
    log(f"dbt_command: {dbt_command}")

    # return the dbt command
    return dbt_command

def load_initial_variables(**kwargs):
    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    firstdayofmonth = now.replace(day=1).strftime('%Y%m%d')
    tempday = now - timedelta(7)

    input_date_parms = Variable.get('gdg_email_inputs', default_var='{}', deserialize_json=True)
    if isinstance(input_date_parms, str):
        try:
            input_date_parms = json.loads(input_date_parms)
        except json.JSONDecodeError:
            input_date_parms = {}

    historic_refresh = str(input_date_parms.get('historic_refresh', 'false')).lower()

    if historic_refresh == 'false':
        start_date = tempday.strftime('%Y%m%d') if now.day < 8 else firstdayofmonth
        end_date   = now.strftime('%Y%m%d')
    else:
        start_date = input_date_parms.get('start_date')
        end_date   = input_date_parms.get('end_date')
        if not start_date or not end_date:
            raise ValueError(
                "historic_refresh=true requires 'start_date' and 'end_date' in gdg_email_inputs"
            )

    dbt_variable_template = "{{START_DATE: '{0}', END_DATE: '{1}', historic_refresh: {2}}}".format(
        start_date, end_date, historic_refresh
    )

    kwargs['ti'].xcom_push(key='dbt_variable_template', value=dbt_variable_template)

def get_customer_id_and_date():

    secret_name = 'sprout-api-token'
    region_name = 'us-east-1'
    
    # extracting auth token from the secret received
    auth_token =  get_secretmanager_parameters(secret_name,'api-token', region_name)  
    
    #defining the api details
    date = datetime.today().strftime('%Y%m%d') 
    url = 'https://api.sproutsocial.com/v1/metadata/client'
    hed = {'Authorization': 'Bearer ' + auth_token}
    response = requests.get(url, headers=hed)

    #extracting customer data from api
    print('\n:::Hitting the API and collecting its response...:::')
    customer_data=response.json()

    # Extract customer_id from the parsed JSON
    customer_id = customer_data['data'][0]['customer_id']


    print('\n:::Extracting required details from the response received:::')
    # Print or use the customer_id variable
    print("Customer ID: ", customer_id)

    return {"customer_id": customer_id,"auth_token":auth_token,"date": date}

# custom function to create a runner/callable object to execute the python scripts directly by Airflow python operator
def make_runner(script_name):
    def _runner():
        runpy.run_path(script_name, run_name="__main__")
    return _runner