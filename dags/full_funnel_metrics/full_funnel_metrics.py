"""
Full Funnel Metrics DBT DAG RUN

"""
################################################################
# Airflow imports
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG
# Python standard libs
from datetime import datetime, timedelta
from dags.common.py.utils.snowflake_sensor import SnowflakeSqlSensor
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.python_operator import BranchPythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
# Custom functions
from dags.common.py.airflow.callbacks.callback_failure_notification import callback_failure_notification
from dags.common.py.notifications.slack_alert import task_success_slack_alert
from dags.full_funnel_metrics.configs import load_config
import os
from dags.full_funnel_metrics.utils import generate_dbt_command, REVMART_SQL, ADOBE_SQL, MARKETABILITY_SQL,POLARIS_SQL, EIOCUSTOMER_SQL





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



    # Dummy tasks.
    start = DummyOperator(task_id='WORKFLOW_START')
    

    task_sensor_revmart = SnowflakeSqlSensor(
            task_id = config["tasks"]["TASK_REVMART_SQLSENSOR"],
            conn_id = SNOWFLAKE_CONNECTION_ID,
            sql = REVMART_SQL,
            timeout=config['SENSOR_TIMEOUT'],
            poke_interval=config['SENSOR_POKE_INTERVAL'],
    )

    task_sensor_adobe = SnowflakeSqlSensor(
            task_id = config["tasks"]["TASK_ADOBE_SQLSENSOR"],
            conn_id = SNOWFLAKE_CONNECTION_ID,
            sql = ADOBE_SQL,
            timeout=config['SENSOR_TIMEOUT'],
            poke_interval=config['SENSOR_POKE_INTERVAL'],
    )

    task_sensor_marketability = SnowflakeSqlSensor(
            task_id = config["tasks"]["TASK_MARKETABILITY_SQLSENSOR"],
            conn_id = SNOWFLAKE_CONNECTION_ID,
            sql = MARKETABILITY_SQL,
            timeout=config['SENSOR_TIMEOUT'],
            poke_interval=config['SENSOR_POKE_INTERVAL'],
    )

    task_sensor_eio = SnowflakeSqlSensor(
            task_id = config["tasks"]["TASK_EIOCUSTOMER_SQLSENSOR"],
            conn_id = SNOWFLAKE_CONNECTION_ID,
            sql = EIOCUSTOMER_SQL,
            timeout=config['SENSOR_TIMEOUT'],
            poke_interval=config['SENSOR_POKE_INTERVAL'],
    )

    task_sensor_polaris = SnowflakeSqlSensor(
            task_id = config["tasks"]["TASK_POLARIS_SQLSENSOR"],
            conn_id = SNOWFLAKE_CONNECTION_ID,
            sql = POLARIS_SQL,
            timeout=config['SENSOR_TIMEOUT'],
            poke_interval=config['SENSOR_POKE_INTERVAL'],
    )


    ffm_dbt_run_workflow = BashOperator(
            task_id=config["tasks"]["TASK_DBT_RUN_FFM"],
            bash_command=generate_dbt_command(
                operation='run',
                model=config['tags']['DBT_TAG_FFM'],
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


    #fun to choose whether to run post checks or not
    def choose_to_run_post_checks_task():
        # default value is False so no post checks will run unless the variable is set to true in Airflow Variables in UI
        val = Variable.get(config['RUN_POST_CHECKS_VARIABLE'], default_var="False").strip().lower()
        return f"{config['TASK_DBT_POST_REFRESH_TESTS_FFM']}" if val == "true" else "SKIPPING_FFM_POST_CHECKS"



    run_post_checks_branch = BranchPythonOperator(
        task_id="RUN_POST_CHECKS_BRANCH",
        python_callable=choose_to_run_post_checks_task,
        dag=dag
    )


    # ffm dbt post refresh checks test task, prints SKIP MESSAGE 
    ffm_post_check_skip_task = BashOperator(
        task_id='SKIPPING_FFM_POST_CHECKS',
        bash_command="echo ':::::: Tests cases skipped for FFM ::::::'",
        dag=dag
    )


    # DBT post refresh checks test command, runs DBT TESTS
    ffm_post_check_test_workflow = BashOperator(
            task_id=config["tasks"]["TASK_DBT_POST_REFRESH_TESTS_FFM"],
            bash_command=generate_dbt_command(
                operation='test',
                model=config['DBT_TAG_FFM_POST_CHECKS'],
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

    # Slack Success Task
    slack_success_alert_task = task_success_slack_alert(dag=dag)

    end = DummyOperator(task_id='WORKFLOW_END')

# Final DAG flow.
start >> [task_sensor_revmart,task_sensor_adobe,task_sensor_marketability,task_sensor_eio,task_sensor_polaris] >>\
ffm_dbt_run_workflow >> run_post_checks_branch >> [ffm_post_check_skip_task,ffm_post_check_test_workflow] >> \
slack_success_alert_task >> end

######################################################################

