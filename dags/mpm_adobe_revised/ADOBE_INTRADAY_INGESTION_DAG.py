################################################################

"""
Adobe Ondemand ingestion Workflow.
"""
# Airflow imports
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook 
from airflow.utils.task_group import TaskGroup
from dags.common.py.aws.cloudformation import Cloudformation
from dags.common.py.spark import spark_config_generator

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from dags.activity_marketo_v2_driver.utility_functions import aws_conn_credentials


# Custom libraries / Settings
from dags.common.py.airflow.tasks.dummy_tasks import AirflowDummyTaskCreator
from dags.mpm_adobe_revised.config import settings
from dags.common.py.notifications.slack_alert import task_fail_slack_alert, task_success_slack_alert
from dags.common.py.airflow.callbacks.callback_failure_notification import callback_failure_notification
from airflow.operators.python_operator import ShortCircuitOperator,PythonOperator
from dags.common.py.airflow.emr_cluster_start import airflow_create_emr_cluster
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from dags.mpm_adobe_revised.subdags.subdag_emr_steps_new import emr_steps_id_stack, emr_steps_sessionization, emr_steps_adobe_conv_stack, emr_steps_adobe_engagement_stack
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# Python libs imports
from datetime import datetime, timedelta,timezone
import os
import json
import boto3
from botocore.exceptions import ClientError, NoCredentialsError


AWS_DEFAULT_REGION = 'us-east-1' 
SNOWFLAKE_CONN_ID = os.environ['SNOWFLAKE_CONN']
dbt_target   = os.environ.get("ENVIRONMENT", "dev")
project_dir = '/usr/local/airflow/dbt/mars_dbt/'
dbt_dir = '/usr/local/airflow/dbt_venv/bin/dbt'
# Cloudformation Class object
cfn_obj = Cloudformation()
# SparkConfigGenerator class object
obj = spark_config_generator.SparkConfigGenerator()
# Get AWS credentials
ACCESS_KEY, SECRET_KEY = aws_conn_credentials()

################################################################
# Dag Object creation.

# Create an ID for the DAG
DAG_ID = 'Adobe_Intraday_Ingestion'

start_dt = datetime(2025, 7, 3)
# Get the current time
now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

# Set of Defaults for the DAG object
default_args = {
    'owner': settings.DAG_OWNER,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': start_dt,
    'on_failure_callback': callback_failure_notification
}



#############################################################
# Tasks.

# Dummy task.

def determine_ingestion_window(**context):
    # Get DAG execution date from Airflow context (UTC)
    execution_date = context['execution_date']
    print('\n\n:::::')
    print("execution_date",execution_date)
    # Load ingestion settings
    ingestion_settings = Variable.get(
        settings.AIRFLOW_VAR_ADOBE_ONDEMAND,
        default_var='{}',
        deserialize_json=True
    )

    ingestion_config = ingestion_settings.get("ingestion", {})

    # Cutoff time defaults
    cutoff_hour = ingestion_config.get("cutoff_hour", 9)
    cutoff_minute = ingestion_config.get("cutoff_min", 55)

    print("Execution time (UTC):", execution_date)
    print("Cutoff time: {:02d}:{:02d} UTC".format(cutoff_hour, cutoff_minute))

    adjusted_execution_date = execution_date + timedelta(hours=4)
    print("adjusted_execution_date", str(adjusted_execution_date))
    print("adjusted_execution_date hour", str(adjusted_execution_date.hour))
    print("adjusted_execution_date minute", str(adjusted_execution_date.minute))
    # Set start/end date based on historic_refresh flag
    if ingestion_config.get('historic_refresh') is True:
        start_date = ingestion_config.get('start_date')
        end_date = ingestion_config.get('end_date')
    else:
        if (adjusted_execution_date.hour > cutoff_hour) or (
                adjusted_execution_date.hour == cutoff_hour and adjusted_execution_date.minute >= cutoff_minute):
            date_str = adjusted_execution_date.strftime('%Y%m%d')

        else:
            date_str = (adjusted_execution_date - timedelta(days=1)).strftime('%Y%m%d')
        start_date = date_str
        end_date = date_str

    # Determine if we use current or previous date
    print("Date will load from {} to {}".format(start_date,end_date))
    print(':::::\n\n')
    # Update and persist back the variable
    ingestion_settings["ingestion"]["start_date"] = start_date
    ingestion_settings["ingestion"]["end_date"] = end_date

    Variable.set(
        settings.AIRFLOW_VAR_ADOBE_ONDEMAND,
        json.dumps(ingestion_settings)
    )



# Get the settings
ingestion_settings = Variable.get(settings.AIRFLOW_VAR_ADOBE_ONDEMAND, default_var={}, deserialize_json=True)
ingestion_config = ingestion_settings.get("ingestion", {})
start_date= ingestion_config.get('start_date')
end_date = ingestion_config.get('end_date')

print(f"::::: ingestion_config: \n{json.dumps(ingestion_settings)}")
# get the EMR Settings
adobe_emr_settings = ingestion_settings.get('emr', {})

# Update the CloudFormation parameters based on settings.
cfn_params = settings.EMR_CLOUDFORMATION_PARMS
cfn_params['MasterInstanceType'] = adobe_emr_settings.get('MasterInstanceType', 'm5.dummy')
cfn_params['CoreInstanceType'] = adobe_emr_settings.get('CoreInstanceType', 'm5.dummy')
cfn_params['CoreNodeCount'] = str(adobe_emr_settings.get('CoreNodeCount', 0))
cfn_params['MinCoreNodeCount'] = str(adobe_emr_settings.get('CoreNodeCount', 0))
cfn_params['MaxCoreNodeCount'] = str(adobe_emr_settings.get('CoreNodeCount', 0))


########################################################

# Function to get date range list
def get_date_range_compact(start_date, end_date):
    """
    Compact version using list comprehension.
    """
    start = datetime.strptime(start_date, '%Y%m%d')
    end = datetime.strptime(end_date, '%Y%m%d')
    days = (end - start).days + 1
    
    return [(start + timedelta(days=i)).strftime('%Y%m%d') for i in range(days)]

########################################################

# Function to check if S3 folder exists and contains only non-empty .snappy.parquet files   
def check_s3_folder_exists(**context):
    """Return True if folder has only non-empty .snappy.parquet files, else raise Exception"""
    
    ## Fetching AWS Creds From conncetions in Airflow's Variable ####
    conn = BaseHook.get_connection('aws_default')  
    aws_access_key = conn.login
    aws_secret_key = conn.password
    
    Session = boto3.Session(
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name='us-east-1'
    )
    
    
    s3_client = Session.client('s3')
    bucket_name = settings.BUCKET_NAME
    start_date = ingestion_config.get('start_date')
    end_date = ingestion_config.get('end_date')


    date_list = get_date_range_compact(start_date, end_date)
    print('\n\n:::::')
    print(f"::: date_list: {date_list}")

    for _dt in date_list:
        prefix = settings.PREFIX + '/' + f'dt={_dt}/'
        
        try:
            # This will check for each day if the folder exists and contains only non-empty .snappy.parquet files
            paginator = s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

            parquet_files = []

            for page in page_iterator:
                if "Contents" not in page:
                    continue
                for obj in page["Contents"]:
                    key = obj["Key"]
                    size = obj["Size"]

                    if key.endswith(".snappy.parquet"):
                    # if key.endswith(".parquet"): #uncomment in case for hourly changes
                        parquet_files.append((key, size))

            # ✅ Check conditions after scanning all files
            if not parquet_files:
                raise Exception(f"No .snappy.parquet files found in 's3://{bucket_name}/{prefix}'")

            empty_files = [f for f, s in parquet_files if s == 0]
            if empty_files:
                raise Exception(
                    f"Some .snappy.parquet files are empty in 's3://{bucket_name}/{prefix}': {empty_files}"
                )
            print(f"✅ Directory 's3://{bucket_name}/{prefix}' contains only non-empty .snappy.parquet files\n\n")
            print(':::::\n\n')

        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                raise Exception(f"S3 bucket '{bucket_name}' does not exist")
            else:
                raise Exception(f"Error accessing S3: {e}")
        except NoCredentialsError:
            raise Exception("AWS credentials not found. Please configure your credentials.")
        
    return True

def del_s3_useless_files():
    """
    THis function would delete Useless files generated by the Spark job.
    if we dont delete these files then the matillion job would fail.
    :return:
    """

    # S3 resource object
    s3 = boto3.resource('s3', region_name='us-east-1', aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY)

    # bucket object
    bucket = s3.Bucket(settings.SPARK_S3_ADOBE_STACK_BUCKET.replace("//", '/').split("/")[1])

    # loop through each of the objects
    for obj in bucket.objects.all():

        # check if the object name ends with $folder$ if so delete these files
        if obj.key[-8:] == "$folder$":
            print("del_s3_useless_files() Deleting S3 Object >>> " + str(obj))

            # issue the command to actually delete the object from s3.
            obj.delete()

def branch_trigger_dag():
    """Branch logic to decide whether to trigger the Marketo DAG."""
    current_hour = datetime.now().hour
    if current_hour >= 13:
        return 'trigger_Marketo_Automated_Ingestion_Intraday'
    else:
        return 'skip_marketo_trigger'

# Create the DAG object and schedule it
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=settings.SCHEDULE_INTERVAL,
    start_date=now,
    max_active_runs=1,
    catchup=False,
    description='incremental adobe data loading on daily basis',
) as dag:
    
    task_start = DummyOperator(task_id="WORKFLOW_START")

    task_determine_start_date = PythonOperator(
        task_id='DETERMINE_ADOBE_INGESTION_WINDOW',
        python_callable=determine_ingestion_window,
        provide_context=True,
        )
    
# Short circuit task - will skip downstream if returns False
    check_folder = ShortCircuitOperator(
        task_id='CHECK_S3_FOLDER',
        python_callable=check_s3_folder_exists,
    )

    with TaskGroup(settings.SUBDAG_EMR_INIT) as subgag_emr_init:
        task_create_emr_cluster = PythonOperator(
            task_id=settings.TASK_CREATE_EMR_CLUSTER,
            python_callable=airflow_create_emr_cluster,
            op_kwargs={
                'emr_cluster_name': settings.EMR_CLUSTER_NAME,
                'cfn_template_name': settings.EMR_CLOUDFORMATION_STACK_NAME,
                'cfn_template_file': settings.EMR_CLOUDFORMATION_TEMPLATE_FILE,
                'cfn_params': cfn_params
            },
            )
        
        task_spark_config_generator = PythonOperator(
            task_id=settings.TASK_SPARK_CONFIG_GENERATOR,
            provide_context=True,
            python_callable=obj.generate_config,
            op_kwargs={
                'no_of_nodes': adobe_emr_settings.get('CoreNodeCount', 0),
                'instance_type': adobe_emr_settings.get('CoreInstanceType', 'm5.dummy')
            },
            )
        task_create_emr_cluster >> task_spark_config_generator


    sessionization_values = emr_steps_sessionization('SESSIONIZATION')
    sessionization_sensor_values = emr_steps_sessionization('SESSIONIZATION_SENSOR')
    id_stack_values = emr_steps_id_stack('ID_STACK')
    id_stack_sensor_values = emr_steps_id_stack('ID_STACK_SENSOR')
    conv_stack_values = emr_steps_adobe_conv_stack('CONVERSION_STACK')
    conv_stack_sensor_values = emr_steps_adobe_conv_stack('CONVERSION_STACK_SENSOR')
    engagement_stack_values = emr_steps_adobe_engagement_stack('ENGAGEMENT_STACK')
    engagement_stack_sensor_values = emr_steps_adobe_engagement_stack('ENGAGEMENT_STACK_SENSOR')

    # Subdag EMR Steps
    with TaskGroup(settings.SUBDAG_EMR_STEPS) as subgag_emr_step:   
        task_list = {}     
        if not ingestion_config.get('skip_sessionization') :
            task_list["emr_sessionization_task"] = EmrAddStepsOperator(**sessionization_values)
            task_list["emr_sessionization_sensor_task"] = EmrStepSensor(**sessionization_sensor_values)
        if not ingestion_config.get('skip_id_stack') :
            task_list["emr_id_stack_task"] = EmrAddStepsOperator(**id_stack_values)
            task_list["emr_id_stack_sensor_task"] = EmrStepSensor(**id_stack_sensor_values)
        if not ingestion_config.get('skip_conv_stack') :
            task_list["emr_conv_stack_task"] = EmrAddStepsOperator(**conv_stack_values)
            task_list["emr_conv_stack_sensor_task"] = EmrStepSensor(**conv_stack_sensor_values)
        if not ingestion_config.get('skip_evt_stack') :
            task_list["emr_evt_stack_task"] = EmrAddStepsOperator(**engagement_stack_values)
            task_list["emr_evt_stack_sensor_task"] = EmrStepSensor(**engagement_stack_sensor_values)

        task_list["task_del_s3_useless_files"] = PythonOperator(
            task_id=settings.TASK_DEL_S3_USELESS_FILES,
            python_callable=del_s3_useless_files,
            )

        task_keys = list(task_list.keys())
        for i in range(len(task_keys) - 1):
            task_list[task_keys[i]] >> task_list[task_keys[i + 1]]
        # emr_sessionization_task >> emr_sessionization_sensor_task >> emr_id_stack_task >> emr_id_stack_sensor_task

    if adobe_emr_settings.get('ShutdownCluster', True):
        # task to delete the emr cluster.
        task_delete_emr_cluster = PythonOperator(
            task_id=settings.TASK_DELETE_EMR_CLUSTER,
            python_callable=cfn_obj.delete_stack,
            op_kwargs={
                'name': settings.EMR_CLOUDFORMATION_STACK_NAME
            },
        )
    else:
        # Dummy task which says EMR Cluster will not be deleted.
        task_delete_emr_cluster = DummyOperator(task_id="DONT_DELETE_EMR")

    # DBT Vars.
    dbt_variable = "'{{START_DATE: {start_date}, END_DATE: {end_date}, DATASOURCE: adobe}}'".format(
        start_date=start_date,
        end_date=end_date
    )
    # DBT Command to load mpm_ds_adobe models.
    command_adobe_workflow = "{dbt_dir} run --vars {dbt_variable} -s {model} --project-dir {project_dir} --profiles-dir {project_dir} --profile {profile} --target {env}"

    # # dbt marketo workflow commamnd
    command_adobe_workflow = command_adobe_workflow.format(dbt_dir=dbt_dir,
                                                    dbt_variable=dbt_variable,
                                                    model=settings.DBT_MODEL_ADOBE_WORKFLOW,
                                                    project_dir=project_dir,
                                                    profile=settings.DBT_PROFILE,
                                                    env=dbt_target)

    # Pull Snowflake password from Airflow Connection and inject into dbt env which will be later used with profiles.yml
    sf_conn = BaseHook.get_connection(settings.SNOWFLAKE_CONN_ID)
    dbt_env = {
        'SNOWFLAKE_PASSWORD': sf_conn.password
    }
    
    task_dbt_adobe_workflow = BashOperator(
        task_id='TASK_DBT_ADOBE_WORKFLOW',
        bash_command=command_adobe_workflow,
        env=dbt_env,
        dag=dag,
    )

    # DBT Command to refresh external tables in snowflake.
    dbt_command = "{dbt_dir} run  -s {model} --project-dir {project_dir} --profiles-dir {project_dir} --profile {profile} --target {env}"\
        .format(dbt_dir=dbt_dir,
                model=settings.DBT_MODEL_EXTTBL_REFRESH,
                project_dir=project_dir,
                profile=settings.DBT_PROFILE,
                env=dbt_target)

    # Call the DBT Workflow to load the data in to Spend Stack.
    task_dbt_exttable_refresh = BashOperator(
        task_id='DBT_EXTTBL_REFRESH',
        bash_command=dbt_command,
        env=dbt_env,
        dag=dag)
    
    # Branch to decide whether to trigger the Marketo DAG
    branch_trigger_task = BranchPythonOperator(
        task_id='branch_trigger_task',
        python_callable=branch_trigger_dag
    )

    # Trigger Marketo_Automated_Ingestion_Intraday DAG
    trigger_marketo_dag = TriggerDagRunOperator(
        task_id='trigger_Marketo_Automated_Ingestion_Intraday',
        trigger_dag_id='Marketo_Automated_Ingestion_Intraday',
        wait_for_completion=False,
        dag=dag
    )

    skip_trigger = DummyOperator(task_id='skip_marketo_trigger')

    # Slack Success Task
    slack_success_alert_task = task_success_slack_alert(dag=dag)

    end = DummyOperator(task_id="end", trigger_rule='none_failed')

# DAG Flow.
task_start >> task_determine_start_date >> check_folder >> subgag_emr_init >> subgag_emr_step >> \
task_delete_emr_cluster >> task_dbt_adobe_workflow >> task_dbt_exttable_refresh >> branch_trigger_task
branch_trigger_task >> [trigger_marketo_dag, skip_trigger]
trigger_marketo_dag >> slack_success_alert_task >> end
skip_trigger >> slack_success_alert_task >> end
# #############################################################
# End of Ondemand Ingestion Workflow.