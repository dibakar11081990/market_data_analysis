"""Full Funnel Metrics DBT DAG

This DAG analyses how allocated budget has been used/unused.
"""

# Standard library
import os
from datetime import datetime, timedelta

# Airflow
from airflow.hooks.base import BaseHook
from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

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
    generate_dbt_command,
)

# ---------------------------------------------------------------------------
# Environment & connections
# ---------------------------------------------------------------------------

ENVIRONMENT = 'dev' if os.environ['ENVIRONMENT'].lower() == 'stg' else os.environ['ENVIRONMENT']
SNOWFLAKE_CONNECTION_ID = os.environ['SNOWFLAKE_CONN']
snowflake_connection = BaseHook.get_connection(SNOWFLAKE_CONNECTION_ID)

SNOWFLAKE_ENV = {
    "SNOWFLAKE_PASSWORD": snowflake_connection.password,
    "SNOWFLAKE_USER": snowflake_connection.login,
    "SNOWFLAKE_ACCOUNT": snowflake_connection.extra_dejson.get(
        'account', "tredence_analytics.us-east-1"
    ),
}

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
# Sensor definitions: (task_id config key, SQL query)
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

    ffm_dbt_run_workflow = BashOperator(
        task_id=config["tasks"]["TASK_DBT_RUN_FFM"],
        bash_command=generate_dbt_command(
            operation='run',
            model=config['tags']['DBT_TAG_FFM'],
            profile=config['dbt_profile'],
            env=SNOWFLAKE_ENV,
        ),
    )

    def choose_to_run_post_checks_task():
        """Return the next task ID based on the Airflow Variable flag."""
        val = Variable.get(config['RUN_POST_CHECKS_VARIABLE'], default_var="False").strip().lower()
        return config['TASK_DBT_POST_REFRESH_TESTS_FFM'] if val == "true" else SKIP_POST_CHECKS_TASK_ID

    run_post_checks_branch = BranchPythonOperator(
        task_id="RUN_POST_CHECKS_BRANCH",
        python_callable=choose_to_run_post_checks_task,
    )

    ffm_post_check_skip_task = BashOperator(
        task_id=SKIP_POST_CHECKS_TASK_ID,
        bash_command="echo ':::::: Tests cases skipped for FFM ::::::'",
    )

    ffm_post_check_test_workflow = BashOperator(
        task_id=config["tasks"]["TASK_DBT_POST_REFRESH_TESTS_FFM"],
        bash_command=generate_dbt_command(
            operation='test',
            model=config['DBT_TAG_FFM_POST_CHECKS'],
            profile=config['dbt_profile'],
            env=SNOWFLAKE_ENV,
        ),
    )

    slack_success_alert_task = task_success_slack_alert(dag=dag)

    end = DummyOperator(task_id='WORKFLOW_END')

    # DAG flow
    start >> sensors >> ffm_dbt_run_workflow
    ffm_dbt_run_workflow >> run_post_checks_branch
    run_post_checks_branch >> [ffm_post_check_skip_task, ffm_post_check_test_workflow]
    [ffm_post_check_skip_task, ffm_post_check_test_workflow] >> slack_success_alert_task >> end
