"""Full Funnel Metrics DBT DAG

This DAG analyses how allocated budget has been used/unused.
"""

# Standard library
import os
from datetime import datetime, timedelta

# Airflow
from airflow.hooks.base import BaseHook
from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

# Cosmos — replaces BashOperator-based dbt execution;
# renders each dbt model matching the tag selector as its own Airflow task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
# maps the existing Airflow Snowflake connection to a dbt profile at runtime,
# avoiding raw credential injection into env vars or bash strings
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Local / custom
from dags.common.py.airflow.callbacks.callback_failure_notification import callback_failure_notification
from dags.common.py.notifications.slack_alert import task_success_slack_alert
from dags.common.py.utils.snowflake_sensor import SnowflakeSqlSensor
from dags.full_funnel_metrics.configs import load_config
from dags.full_funnel_metrics.utils import (
    ADOBE_SQL,
    EIOCUSTOMER_SQL,
    MARKETABILITY_SQL,
    POLARIS_SQL,
    REVMART_SQL,
)

# ---------------------------------------------------------------------------
# Environment & connections
# ---------------------------------------------------------------------------

ENVIRONMENT = 'dev' if os.environ['ENVIRONMENT'].lower() == 'stg' else os.environ['ENVIRONMENT']
SNOWFLAKE_CONNECTION_ID = os.environ['SNOWFLAKE_CONN']
snowflake_connection = BaseHook.get_connection(SNOWFLAKE_CONNECTION_ID)

SKIP_POST_CHECKS_TASK_ID = "SKIPPING_FFM_POST_CHECKS"

# ---------------------------------------------------------------------------
# DAG config & defaults
# ---------------------------------------------------------------------------

config = load_config()

default_args = {
    'owner': config["dag"]["owner"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': callback_failure_notification,
}

# ---------------------------------------------------------------------------
# Cosmos configuration
# ---------------------------------------------------------------------------

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

# points cosmos at the dbt project on disk
project_config = ProjectConfig(
    dbt_project_path="/usr/local/airflow/dbt/mars_dbt/",
)

# points cosmos at the dbt binary inside the virtual environment
execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
)


def dbt_task_group(task_key: str, tag_key: str) -> DbtTaskGroup:
    """Factory that binds the three shared cosmos configs; call-sites only pass what varies."""
    return DbtTaskGroup(
        group_id=config["tasks"][task_key],
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=[config['tags'][tag_key]]),
    )


# ---------------------------------------------------------------------------
# Sensor definitions: (task_id, SQL query)
# ---------------------------------------------------------------------------

SENSOR_SPECS = [
    (config["tasks"]["TASK_REVMART_SQLSENSOR"],       REVMART_SQL),
    (config["tasks"]["TASK_ADOBE_SQLSENSOR"],         ADOBE_SQL),
    (config["tasks"]["TASK_MARKETABILITY_SQLSENSOR"], MARKETABILITY_SQL),
    (config["tasks"]["TASK_EIOCUSTOMER_SQLSENSOR"],   EIOCUSTOMER_SQL),
    (config["tasks"]["TASK_POLARIS_SQLSENSOR"],       POLARIS_SQL),
]

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id=config['dag']['id'],
    default_args=default_args,
    schedule_interval=config['dag']['schedule_interval'],
    start_date=datetime(year=2026, month=3, day=1),
    max_active_runs=1,
    catchup=False,
) as dag:

    start = DummyOperator(task_id='WORKFLOW_START')

    sensors = [
        SnowflakeSqlSensor(
            task_id=task_id,
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql=sql,
            timeout=config['SENSOR_TIMEOUT'],
            poke_interval=config['SENSOR_POKE_INTERVAL'],
        )
        for task_id, sql in SENSOR_SPECS
    ]

    ffm_dbt_run = dbt_task_group("TASK_DBT_RUN_FFM", "DBT_TAG_FFM")

    def choose_to_run_post_checks_task():
        """Return the next task ID based on the Airflow Variable flag."""
        val = Variable.get(config['RUN_POST_CHECKS_VARIABLE'], default_var="False").strip().lower()
        return config['tasks']['TASK_DBT_POST_REFRESH_TESTS_FFM'] if val == "true" else SKIP_POST_CHECKS_TASK_ID

    run_post_checks_branch = BranchPythonOperator(
        task_id="RUN_POST_CHECKS_BRANCH",
        python_callable=choose_to_run_post_checks_task,
    )

    ffm_post_check_skip = DummyOperator(task_id=SKIP_POST_CHECKS_TASK_ID)

    # Gate task: branch operator targets this task_id; the DbtTaskGroup follows it
    # so it inherits the "skipped" state when the branch goes to ffm_post_check_skip.
    ffm_post_check_gate = DummyOperator(
        task_id=config["tasks"]["TASK_DBT_POST_REFRESH_TESTS_FFM"],
    )

    ffm_post_check_tests = dbt_task_group("TASK_POST_CHECKS_GROUP", "DBT_TAG_FFM_POST_CHECKS")

    slack_success_alert_task = task_success_slack_alert(dag=dag)

    end = DummyOperator(task_id='WORKFLOW_END')

    # DAG flow
    start >> sensors >> ffm_dbt_run >> run_post_checks_branch
    run_post_checks_branch >> [ffm_post_check_skip, ffm_post_check_gate]
    ffm_post_check_gate >> ffm_post_check_tests
    [ffm_post_check_skip, ffm_post_check_tests] >> slack_success_alert_task >> end
