"""
# Refactored Dbt models Regulated: sprout_api_ingestion directory
# This version uses dynamic task generation to reduce code duplication
"""

from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook

from datetime import datetime, timedelta
import os

# Custom imports
from dags.common.py.airflow.callbacks.callback_failure_notification import callback_failure_notification
from dags.common.py.notifications.slack_alert import task_success_slack_alert
from dags.sprout_api_ingestion.configs import load_config
from dags.sprout_api_ingestion.utils import generate_dbt_command, make_runner

# Script imports
from dags.sprout_api_ingestion.python_scripts.o1_sprout_customer_api_ingestion.s1_customer_data_api_extract_load import customer_data_extract_load
from dags.sprout_api_ingestion.python_scripts.o1_sprout_customer_api_ingestion.s2_customer_tag_api_extract_load import customer_tag_data_extract_load
from dags.sprout_api_ingestion.python_scripts.o1_sprout_customer_api_ingestion.s3_customer_groups_api_extract_load import customer_group_data_extract_load
from dags.sprout_api_ingestion.python_scripts.o1_sprout_customer_api_ingestion.s4_customer_users_api_extract_load import customer_users_data_extract_load
from dags.sprout_api_ingestion.python_scripts.o1_sprout_customer_api_ingestion.s5_customer_topics_api_extract_load import customer_topics_data_extract_load









ENVIRONMENT = 'dev' if os.environ['ENVIRONMENT'].lower() == 'stg' else os.environ['ENVIRONMENT']
SNOWFLAKE_CONNECTION_ID = os.environ['SNOWFLAKE_CONN']
snowflake_connection = BaseHook.get_connection(SNOWFLAKE_CONNECTION_ID)

now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=7)
config = load_config()

default_args = {
    'owner': config["dag"]["owner"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'snowflake_conn_id': SNOWFLAKE_CONNECTION_ID,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': callback_failure_notification
}

# ==================== HELPER FUNCTIONS ====================

def create_python_operator_from_callable(task_id, callable_func, dag):
    """Factory function to create PythonOperator with consistent config"""
    return PythonOperator(
        task_id=task_id,
        python_callable=callable_func,
        provide_context=True,
        dag=dag,
    )

def create_python_operator_from_script(task_id, script_path, dag):
    """Factory function for script-based PythonOperators"""
    return PythonOperator(
        task_id=task_id,
        python_callable=make_runner(script_path),
        provide_context=True,
        dag=dag,
    )

def create_dbt_operator(task_id, model_tag, dag):
    """Factory function for DBT BashOperators with consistent config"""
    dbt_env = {
        "SNOWFLAKE_PASSWORD": snowflake_connection.password,
        "SNOWFLAKE_USER": snowflake_connection.login,
        "SNOWFLAKE_ACCOUNT": snowflake_connection.extra_dejson['account'],
    }
    return BashOperator(
        task_id=task_id,
        bash_command=generate_dbt_command(operation='run', model=model_tag, profile='mpm'),
        env=dbt_env,
    )

# ==================== DAG DEFINITION ====================

with DAG(
    dag_id=config['dag']['id'],
    default_args=default_args,
    schedule_interval=config['dag']['schedule_interval'],
    start_date=now,
    max_active_runs=1,
    description='This Dag Ingests Sprout data using API for brands. The flow loads currentDate only into the Main Table.',
) as dag:

    start = DummyOperator(task_id="START")

    # ==================== TASK GROUP 1: CUSTOMER API INGESTION ====================
    with TaskGroup(group_id="Orc1_Sprout_Customer_API_Ingestion") as o1_sprout_customer_api_ingestion:
        
        # Define tasks using direct callables
        customer_tasks = {
            'CUSTOMER_DATA_API_EXTRACT_LOAD': customer_data_extract_load,
            'CUSTOMER_TAG_DATA_EXTRACT_LOAD': customer_tag_data_extract_load,
            'CUSTOMER_GROUPS_API_EXTRACT_LOAD': customer_group_data_extract_load,
            'CUSTOMER_USERS_API_EXTRACT_LOAD': customer_users_data_extract_load,
            'CUSTOMER_TOPICS_DATA_EXTRACT_LOAD': customer_topics_data_extract_load,
        }
        
        for task_id, callable_func in customer_tasks.items():
            create_python_operator_from_callable(task_id, callable_func, dag)

    # ==================== TASK GROUP 2: PROFILE INGESTION ====================
    with TaskGroup(group_id="orc2_sprout_customer_profile_ingestion") as o2_sprout_customer_profile_ingestion:
        
        # Define profile tasks with script paths
        profile_tasks = {
            'X_PROFILE_API_EXTRACT_LOAD': 'dags/sprout_api_ingestion/python_scripts/o2_sprout_customer_profile_ingestion/s1_x_profile_api_extract_load.py',
            'FACEBOOK_PROFILE_API_EXTRACT_LOAD': 'dags/sprout_api_ingestion/python_scripts/o2_sprout_customer_profile_ingestion/s2_facebook_profile_api_extract_load.py',
            'INSTAGRAM_PROFILE_API_EXTRACT_LOAD': 'dags/sprout_api_ingestion/python_scripts/o2_sprout_customer_profile_ingestion/s3_instagram_profile_api_extract_load.py',
            'LINKEDIN_PROFILE_API_EXTRACT_LOAD': 'dags/sprout_api_ingestion/python_scripts/o2_sprout_customer_profile_ingestion/s4_linkedIn_profile_api_extract_load.py',
            'YOUTUBE_PROFILE_API_EXTRACT_LOAD': 'dags/sprout_api_ingestion/python_scripts/o2_sprout_customer_profile_ingestion/s5_youTube_profile_api_extract_load.py',
            'TIKTOK_PROFILE_API_EXTRACT_LOAD': 'dags/sprout_api_ingestion/python_scripts/o2_sprout_customer_profile_ingestion/s6_tiktok_profile_api_extract_load.py',
        }
        
        for task_id, script_path in profile_tasks.items():
            create_python_operator_from_script(task_id, script_path, dag)

    # ==================== TASK GROUP 3: POST INGESTION ====================
    with TaskGroup(group_id="orc3_sprout_customer_post_ingestion") as o3_sprout_customer_post_ingestion:
        
        # Define post tasks with script paths
        post_tasks = {
            'X_POST_API_EXTRACT_LOAD': 'dags/sprout_api_ingestion/python_scripts/o3_sprout_customer_post_ingestion/s1_x_post_api_extract_load.py',
            'FACEBOOK_POST_API_EXTRACT_LOAD': 'dags/sprout_api_ingestion/python_scripts/o3_sprout_customer_post_ingestion/s2_facebook_post_api_extract_load.py',
            'INSTAGRAM_POST_API_EXTRACT_LOAD': 'dags/sprout_api_ingestion/python_scripts/o3_sprout_customer_post_ingestion/s3_instagram_post_api_extract_load.py',
            'LINKEDIN_POST_API_EXTRACT_LOAD': 'dags/sprout_api_ingestion/python_scripts/o3_sprout_customer_post_ingestion/s4_linkedIn_post_api_extract_load.py',
            'YOUTUBE_POST_API_EXTRACT_LOAD': 'dags/sprout_api_ingestion/python_scripts/o3_sprout_customer_post_ingestion/s5_youtube_post_api_extract_load.py',
            'TIKTOK_POST_API_EXTRACT_LOAD': 'dags/sprout_api_ingestion/python_scripts/o3_sprout_customer_post_ingestion/s6_tiktok_post_api_extract_load.py',
        }
        
        for task_id, script_path in post_tasks.items():
            create_python_operator_from_script(task_id, script_path, dag)

    # ==================== DELETE FROM PROD ====================
    o4_delete_from_prod = create_python_operator_from_script(
        'Prod_Data_Delete',
        'dags/sprout_api_ingestion/python_scripts/o4_delete_from_prod.py',
        dag
    )

    # ==================== DBT TASKS ====================
    with TaskGroup(group_id="dbt_run_tasks_group") as dbt_run_tasks:
        
        # Define DBT tasks with model tags
        dbt_tasks = {
            'DBT_RUN_CUSTOMER_API_INGESTION': 'tag:sprout_customer_api_ingestion',
            'DBT_RUN_CUSTOMER_PROFILE_INGESTION': 'tag:sprout_customer_profile_ingestion',
            'DBT_RUN_CUSTOMER_POST_INGESTION': 'tag:sprout_customer_post_ingestion',
            'DBT_RUN_FACT_INGESTION': 'tag:sprout_fact_ingestion',
        }
        
        dbt_operators = {}
        for task_id, model_tag in dbt_tasks.items():
            dbt_operators[task_id] = create_dbt_operator(task_id, model_tag, dag)
        
        # Define task order
        (dbt_operators['DBT_RUN_CUSTOMER_API_INGESTION'] >> 
         dbt_operators['DBT_RUN_CUSTOMER_PROFILE_INGESTION'] >> 
         dbt_operators['DBT_RUN_CUSTOMER_POST_INGESTION'] >> 
         dbt_operators['DBT_RUN_FACT_INGESTION'])

    # ==================== FINAL TASKS ====================
    slack_success_alert_task = task_success_slack_alert(dag=dag)
    end = DummyOperator(task_id="END")

    # ==================== DAG FLOW ====================
    start >> o1_sprout_customer_api_ingestion >> \
        o2_sprout_customer_profile_ingestion >> \
        o3_sprout_customer_post_ingestion >> \
        o4_delete_from_prod >> \
        dbt_run_tasks >> \
        slack_success_alert_task >> end