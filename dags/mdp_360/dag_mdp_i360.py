"""
mdp - marketing and performance dashboard

This dag analyses how allocated budget has been used/un-used.
"""
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
from dags.mdp_360.utils import generate_dbt_command, upload_sftp_files_to_s3_to_sf_for_mdp_i360_new, sftp_move_files_out







ENVIRONMENT = 'dev' if os.environ['ENVIRONMENT'].lower() == 'stg' else os.environ['ENVIRONMENT']
SNOWFLAKE_CONNECTION_ID = os.environ['SNOWFLAKE_CONN']
snowflake_connection = BaseHook.get_connection(SNOWFLAKE_CONNECTION_ID)

config = load_config()

# Shared Snowflake credentials passed to every dbt BashOperator
snowflake_env = {
    "SNOWFLAKE_PASSWORD": snowflake_connection.password,
    "SNOWFLAKE_USER": snowflake_connection.login,
    "SNOWFLAKE_ACCOUNT": snowflake_connection.extra_dejson.get(
        'account', "tredence_analytics.us-east-1"
    ),
}

default_args = {
    'owner': config["dag"]["owner"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': callback_failure_notification,
}


def dbt_run_operator(task_key, tag_key, doc_md=None):
    """Create a BashOperator that runs a dbt model by tag."""
    return BashOperator(
        task_id=config["tasks"][task_key],
        bash_command=generate_dbt_command(
            operation='run',
            model=config['tags'][tag_key],
            profile=config['dbt_profile'],
            env=snowflake_env,
        ),
        doc_md=doc_md,
    )


with DAG(
    dag_id=config['dag']['id'],
    default_args=default_args,
    schedule_interval=config['dag']['schedule_interval'],
    start_date=datetime(year=2026, month=3, day=1),
    max_active_runs=1,
    catchup=False,
    doc_md=__doc__,
) as dag:

    start = DummyOperator(task_id="START")

    # STEP 1 — Load SFTP files into S3 then Snowflake
    with TaskGroup(group_id="1_SFTP_data_load") as mdp_load_sftp:

        sftp_data_load_script = PythonOperator(
            task_id=config["tasks"]["TASK_SFTP_2_S3_2_SF_LOAD"],
            python_callable=upload_sftp_files_to_s3_to_sf_for_mdp_i360_new,
            provide_context=True,
            execution_timeout=timedelta(minutes=60),
        )

        sftp_data_integration = dbt_run_operator(
            task_key="TASK_SFTP_DATA_INTEGRATION",
            tag_key="DBT_TAG_SFTP_DATA_INTEGRATION",
        )

        sftp_move_files_out_script = PythonOperator(
            task_id=config["tasks"]["TASK_SFTP_MOVE_FILES_TO_OUT"],
            python_callable=sftp_move_files_out,
            provide_context=True,
            execution_timeout=timedelta(minutes=30),
        )

        sftp_data_load_script >> sftp_data_integration >> sftp_move_files_out_script

    # STEP 2 — Rename columns
    rename_columns = dbt_run_operator(
        task_key="TASK_RENAME_COLUMNS",
        tag_key="DBT_TAG_RENAME_COLS",
    )

    # STEP 3 — Filter multiples
    filter_multiples = dbt_run_operator(
        task_key="TASK_FILTER_MULTIPLES",
        tag_key="DBT_TAG_FILTER_MULTIPLIES",
    )

    # STEP 4 — Export i360 data to staging table for PBI
    mdp_i360_new_export_for_pbi_stg = dbt_run_operator(
        task_key="TASK_MDP_EXPORT_FOR_PBI_STG",
        tag_key="DBT_TAG_EXPORT_FOR_PBI_STG",
        doc_md="Take i360 tool data and create a stage table.",
    )

    # STEP 5 — Extract legacy QV data per business rules
    mdp_i360_new_legacy_qv_data = dbt_run_operator(
        task_key="TASK_MDP_LEGACY_QV_DATA",
        tag_key="DBT_TAG_LEGACY_QV_DATA",
        doc_md="Use legacy_qv_data static historical data in Snowflake and extract as per business rules.",
    )

    # STEP 6 — Union legacy QV data with e2open data
    mdp_i360_new_legacy_qv_data_and_e2open = dbt_run_operator(
        task_key="TASK_MDP_LEGACY_QV_DATA_AND_E2OPEN",
        tag_key="DBT_TAG_MDP_LEGACY_QV_DATA_AND_E2OPEN",
        doc_md="Union legacy_qv_data with e2open data.",
    )

    # STEP 7 — Union legacy, e2open, and i360 data
    mdp_i360_new_export_with_legacy_e2 = dbt_run_operator(
        task_key="TASK_MDP_EXPORT_WITH_LEGACY_E2",
        tag_key="DBT_TAG_MDP_EXPORT_WITH_LEGACY_E2",
        doc_md="Union legacy_qv_data, e2open, and i360 data.",
    )

    # STEP 8 — Build final fact table for PBI
    mdp_i360_new_export_for_pbi = dbt_run_operator(
        task_key="TASK_MDP_NEW_EXPORT_FOR_PBI",
        tag_key="DBT_TAG_MDP_NEW_EXPORT_FOR_PBI",
        doc_md="Create final fact table.",
    )

    slack_success_alert_task = task_success_slack_alert(dag=dag)

    end = DummyOperator(task_id="END")

    # DAG flow
    (
        start
        >> mdp_load_sftp
        >> rename_columns
        >> filter_multiples
        >> mdp_i360_new_export_for_pbi_stg
        >> mdp_i360_new_legacy_qv_data
        >> mdp_i360_new_legacy_qv_data_and_e2open
        >> mdp_i360_new_export_with_legacy_e2
        >> mdp_i360_new_export_for_pbi
        >> slack_success_alert_task
        >> end
    )
