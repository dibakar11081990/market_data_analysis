"""
mdp - marketing and performance dashboard

This dag analyses how allocated budget has been used/un-used.
"""
# Airflow imports
from airflow.models import Variable
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
# cosmos imports — replaces BashOperator-based dbt execution;
# renders each dbt model matching the tag selector as its own Airflow task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
# maps the existing Airflow Snowflake connection to a dbt profile at runtime,
# avoiding raw credential injection into env vars or bash strings
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
# Python standard libs
from datetime import datetime, timedelta
import os
# Custom functions — generate_dbt_command removed; cosmos replaces it
from dags.common.py.airflow.callbacks.callback_failure_notification import callback_failure_notification
from dags.common.py.notifications.slack_alert import task_success_slack_alert
from dags.mdp_360.configs import load_config
from dags.mdp_360.utils import upload_sftp_files_to_s3_to_sf_for_mdp_i360_new, sftp_move_files_out


ENVIRONMENT = 'dev' if os.environ['ENVIRONMENT'].lower() == 'stg' else os.environ['ENVIRONMENT']
SNOWFLAKE_CONNECTION_ID = os.environ['SNOWFLAKE_CONN']
snowflake_connection = BaseHook.get_connection(SNOWFLAKE_CONNECTION_ID)

config = load_config()

# snowflake_env dict removed — cosmos reads credentials from the Airflow connection
# directly via SnowflakeUserPasswordProfileMapping, so passwords are never exposed
# in operator environment variables or bash command strings

# tells cosmos which Airflow connection to use and how to map it to a dbt Snowflake profile
profile_config = ProfileConfig(
    profile_name=config['dbt_profile'],
    target_name=ENVIRONMENT,
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id=SNOWFLAKE_CONNECTION_ID,
        profile_args={
            "account": snowflake_connection.extra_dejson.get(
                'account', "tredence_analytics.us-east-1"
            )
        },
    ),
)

# points cosmos at the dbt project on disk — same path previously hard-coded
# inside generate_dbt_command in utils/helpers.py
project_config = ProjectConfig(
    dbt_project_path="/usr/local/airflow/dbt/mars_dbt/",
)

# points cosmos at the dbt binary inside the virtual environment
execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
)

default_args = {
    'owner': config["dag"]["owner"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': callback_failure_notification,
}


def dbt_task_group(task_key: str, tag_key: str) -> DbtTaskGroup:
    # factory that binds the three shared cosmos configs so call-sites only
    # pass what actually varies: the task name and the dbt tag selector
    return DbtTaskGroup(
        group_id=config["tasks"][task_key],
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=[config['tags'][tag_key]]),
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

        sftp_data_integration = dbt_task_group("TASK_SFTP_DATA_INTEGRATION", "DBT_TAG_SFTP_DATA_INTEGRATION")

        sftp_move_files_out_script = PythonOperator(
            task_id=config["tasks"]["TASK_SFTP_MOVE_FILES_TO_OUT"],
            python_callable=sftp_move_files_out,
            provide_context=True,
            execution_timeout=timedelta(minutes=30),
        )

        sftp_data_load_script >> sftp_data_integration >> sftp_move_files_out_script

    # STEP 2 — Rename columns
    rename_columns = dbt_task_group("TASK_RENAME_COLUMNS", "DBT_TAG_RENAME_COLS")

    # STEP 3 — Filter multiples
    filter_multiples = dbt_task_group("TASK_FILTER_MULTIPLES", "DBT_TAG_FILTER_MULTIPLIES")

    # STEP 4 — Export i360 data to staging table for PBI
    mdp_i360_new_export_for_pbi_stg = dbt_task_group("TASK_MDP_EXPORT_FOR_PBI_STG", "DBT_TAG_EXPORT_FOR_PBI_STG")

    # STEP 5 — Extract legacy QV data per business rules
    mdp_i360_new_legacy_qv_data = dbt_task_group("TASK_MDP_LEGACY_QV_DATA", "DBT_TAG_LEGACY_QV_DATA")

    # STEP 6 — Union legacy QV data with e2open data
    mdp_i360_new_legacy_qv_data_and_e2open = dbt_task_group("TASK_MDP_LEGACY_QV_DATA_AND_E2OPEN", "DBT_TAG_MDP_LEGACY_QV_DATA_AND_E2OPEN")

    # STEP 7 — Union legacy, e2open, and i360 data
    mdp_i360_new_export_with_legacy_e2  = dbt_task_group("TASK_MDP_EXPORT_WITH_LEGACY_E2", "DBT_TAG_MDP_EXPORT_WITH_LEGACY_E2")

    # STEP 8 — Build final fact table for PBI
    mdp_i360_new_export_for_pbi = dbt_task_group("TASK_MDP_NEW_EXPORT_FOR_PBI", "DBT_TAG_MDP_NEW_EXPORT_FOR_PBI")

    slack_success_alert_task = task_success_slack_alert(dag=dag)

    end = DummyOperator(task_id="END")

    # DAG flow unchanged — DbtTaskGroup participates in >> chaining like any operator
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
