"""
mdp - marketing and performance dashboard

"""
################################################################
# Airflow imports
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
# Python standard libs
from datetime import datetime, timedelta
import os
# Custom functions
from dags.common.py.airflow.callbacks.callback_failure_notification import callback_failure_notification
from dags.common.py.notifications.slack_alert import task_success_slack_alert
from dags.mdp_360.configs import load_config
from dags.mdp_360.utils import generate_dbt_command,upload_sftp_files_to_s3_to_sf_for_mdp_i360_new, sftp_move_files_out







dag_doc="""
    This dag is to analyse how allocated budget has been used/un-used
"""

ENVIRONMENT = 'dev' if os.environ['ENVIRONMENT'].lower() == 'stg' else os.environ['ENVIRONMENT']
SNOWFLAKE_CONNECTION_ID = os.environ['SNOWFLAKE_CONN']
snowflake_connection = BaseHook.get_connection(SNOWFLAKE_CONNECTION_ID)

# Get the current time
now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=7)

config = load_config()

default_args = {
    'owner': config["dag"]["owner"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': callback_failure_notification
}



with DAG(
    dag_id=config['dag']['id'],
    default_args=default_args,
    schedule_interval=config['dag']['schedule_interval'],
    start_date=datetime(year=2026, month=3, day=1),
    max_active_runs=1,
    catchup=False,
) as dag:
    
    start = DummyOperator(task_id="START")

    # STEP1
    # o1_mdp_load_sftp
    # this group loads files from SFTP to S3 and then to Snowflake
    with TaskGroup(group_id="1_SFTP_data_load") as mdp_load_sftp:
        
        
        # sftp_2_s3_2_sf_load_mdpi360new
        # loads data from sftp>s3>snowflake
        sftp_data_load_script = PythonOperator(
            task_id=config["tasks"]["TASK_SFTP_2_S3_2_SF_LOAD"],
            python_callable=upload_sftp_files_to_s3_to_sf_for_mdp_i360_new,
            provide_context=True,
            execution_timeout=timedelta(minutes=60)
         )
        
        # SFTP data Inegration Dbt run
        sftp_data_integration = BashOperator(
                task_id=config["tasks"]["TASK_SFTP_DATA_INTEGRATION"],
                bash_command=generate_dbt_command(
                    operation='run',
                    model=config['tags']['DBT_TAG_SFTP_DATA_INTEGRATION'],
                    profile=config['dbt_profile'],
                    # dbt_variable_template="{{ ti.xcom_pull("
                    #     f"task_ids='{config['tasks']['TASK_GDG_EMAIL_GETDATE_TASK']}', "
                    #     "key='dbt_variable_template') }}"  
                    # ),
                    env={
                        "SNOWFLAKE_PASSWORD": snowflake_connection.password,
                        "SNOWFLAKE_USER": snowflake_connection.login,
                        "SNOWFLAKE_ACCOUNT": snowflake_connection.extra_dejson.get(
                            'account', "tredence_analytics.us-east-1"
                        ),
                    },
                )
            )
        
        # Sftp_move_files_out
        # move data from in/ dir to out/ dir in SFTP
        sftp_move_files_out_script = PythonOperator(
            task_id=config["tasks"]["TASK_SFTP_MOVE_FILES_TO_OUT"],
            python_callable=sftp_move_files_out,
            provide_context=True,
            dag=dag,
            execution_timeout=timedelta(minutes=30)
        )
    
        
    # Define the task dependencies within the TaskGroup
    sftp_data_load_script  >> sftp_data_integration >> sftp_move_files_out_script

        
    # STEP2
    rename_columns = BashOperator(
            task_id=config["tasks"]["TASK_RENAME_COLUMNS"],
            bash_command=generate_dbt_command(
                operation='run',
                model=config['tags']['DBT_TAG_RENAME_COLS'],
                profile=config['dbt_profile'],
                env={
                    "SNOWFLAKE_PASSWORD": snowflake_connection.password,
                    "SNOWFLAKE_USER": snowflake_connection.login,
                    "SNOWFLAKE_ACCOUNT": snowflake_connection.extra_dejson.get(
                        'account', "tredence_analytics.us-east-1"
                    ),
                },
            )
        )
    
        
    # STEP3
    filter_multiples = BashOperator(
            task_id=config["tasks"]["TASK_RENAME_COLUMNS"],
            bash_command=generate_dbt_command(
                operation='run',
                model=config['tags']['DBT_TAG_FILTER_MULTIPLIES'],
                profile=config['dbt_profile'],
                env={
                    "SNOWFLAKE_PASSWORD": snowflake_connection.password,
                    "SNOWFLAKE_USER": snowflake_connection.login,
                    "SNOWFLAKE_ACCOUNT": snowflake_connection.extra_dejson.get(
                        'account', "tredence_analytics.us-east-1"
                    ),
                },
            )
        )
    
    # STEP4
    mdp_i360_new_export_for_pbi_stg = BashOperator(
            task_id=config["tasks"]["TASK_MDP_EXPORT_FOR_PBI_STG"],
            bash_command=generate_dbt_command(
                operation='run',
                model=config['tags']['DBT_TAG_EXPORT_FOR_PBI_STG'],
                profile=config['dbt_profile'],
                env={
                    "SNOWFLAKE_PASSWORD": snowflake_connection.password,
                    "SNOWFLAKE_USER": snowflake_connection.login,
                    "SNOWFLAKE_ACCOUNT": snowflake_connection.extra_dejson.get(
                        'account', "tredence_analytics.us-east-1"
                    ),
                },
            ),
            doc_md = """
                    take i360 tool data and create a stage table
                    """         
        )
    
    
    # STEP5
    mdp_i360_new_legacy_qv_data = BashOperator(
            task_id=config["tasks"]["TASK_MDP_LEGACY_QV_DATA"],
            bash_command=generate_dbt_command(
                operation='run',
                model=config['tags']['DBT_TAG_LEGACY_QV_DATA'],
                profile=config['dbt_profile'],
                env={
                    "SNOWFLAKE_PASSWORD": snowflake_connection.password,
                    "SNOWFLAKE_USER": snowflake_connection.login,
                    "SNOWFLAKE_ACCOUNT": snowflake_connection.extra_dejson.get(
                        'account', "tredence_analytics.us-east-1"
                    ),
                },
            ),
            doc_md = """
                    use legacy_qv_data static historical data in snowflake and extract data as per business
                    """
        )

    
    # STEP6
    mdp_i360_new_legacy_qv_data_and_e2open = BashOperator(
            task_id=config["tasks"]["TASK_MDP_LEGACY_QV_DATA_AND_E2OPEN"],
            bash_command=generate_dbt_command(
                operation='run',
                model=config['tags']['DBT_TAG_MDP_LEGACY_QV_DATA_AND_E2OPEN'],
                profile=config['dbt_profile'],
                env={
                    "SNOWFLAKE_PASSWORD": snowflake_connection.password,
                    "SNOWFLAKE_USER": snowflake_connection.login,
                    "SNOWFLAKE_ACCOUNT": snowflake_connection.extra_dejson.get(
                        'account', "tredence_analytics.us-east-1"
                    ),
                },
            ),
            doc_md = """
                    legacy_qv_data union e2o open data 
                    """
        )

    
    # STEP7
    mdp_i360_new_export_with_legacy_e2 = BashOperator(
            task_id=config["tasks"]["TASK_MDP_EXPORT_WITH_LEGACY_E2"],
            bash_command=generate_dbt_command(
                operation='run',
                model=config['tags']['DBT_TAG_MDP_EXPORT_WITH_LEGACY_E2'],
                profile=config['dbt_profile'],
                env={
                    "SNOWFLAKE_PASSWORD": snowflake_connection.password,
                    "SNOWFLAKE_USER": snowflake_connection.login,
                    "SNOWFLAKE_ACCOUNT": snowflake_connection.extra_dejson.get(
                        'account', "tredence_analytics.us-east-1"
                    ),
                },
            ),
            doc_md = """
                    legacy_qv_data union e2o union i360 data
                    """
        )
    
    
    # STEP8
    mdp_i360_new_export_for_pbi = BashOperator(
            task_id=config["tasks"]["TASK_MDP_NEW_EXPORT_FOR_PBI"],
            bash_command=generate_dbt_command(
                operation='run',
                model=config['tags']['DBT_TAG_MDP_NEW_EXPORT_FOR_PBI'],
                profile=config['dbt_profile'],
                env={
                    "SNOWFLAKE_PASSWORD": snowflake_connection.password,
                    "SNOWFLAKE_USER": snowflake_connection.login,
                    "SNOWFLAKE_ACCOUNT": snowflake_connection.extra_dejson.get(
                        'account', "tredence_analytics.us-east-1"
                    ),
                },
            ),
            doc_md = """
                    create final fact table
                    """
        )


    # Slack Success Task
    slack_success_alert_task = task_success_slack_alert(dag=dag)

    end = DummyOperator(task_id="END")




# Final DAG flow.
start \
    >> mdp_load_sftp \
    >> rename_columns \
    >> filter_multiples \
    >> mdp_i360_new_export_for_pbi_stg \
    >> mdp_i360_new_legacy_qv_data \
    >> mdp_i360_new_legacy_qv_data_and_e2open \
    >> mdp_i360_new_export_with_legacy_e2 \
    >> mdp_i360_new_export_for_pbi \
    >> slack_success_alert_task \
>> end


