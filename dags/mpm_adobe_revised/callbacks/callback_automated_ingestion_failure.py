#############################################################################
"""
A callback for handling the Automated Ingestion Failure
"""
# Airflow Imports
from airflow.providers.http.operators.http import HttpOperator
# custom functions and settings imports
from airflow.operators.python_operator import PythonOperator
from dags.common.py.airflow.callbacks.callback_failure_notification import callback_failure_notification
from dags.mpm_adobe_revised.config import settings
from dags.mpm_adobe_revised.subdags.subdag_automated_cleanup import reset_airflow_settings



############################################################################


def callback_automated_ingestion_failure(context):
    """
    Callback function if any of the tasks fail for automated ingestion.
    :param context: Context object being passed in by airflow.
    :return:
    """

    # Handle the Failure notification
    callback_failure_notification(context=context)

    # runid jinja template.
    runid_jinja_template = "{{ var.json.{var}.ingestion.get('job_run_id', 1) }}".format(var=settings.AIRFLOW_VAR_ADOBE_AUTOMATED). \
        replace("{", "{{").replace("}", "}}")

    # Task to Update the RDS Stating the job has finished via the api call.
    task_update_rds = HttpOperator(
        task_id=settings.TASK_UPDATE_RDS,
        endpoint='job/adp_update?runid='+runid_jinja_template+'&mars_process_status=PROCESSING_RETURNED_ERROR',
        response_check=lambda response: response.status_code == 200,
    )

    # Task to reset the airflow settings.
    task_reset_airflow_settings = PythonOperator(
        task_id=settings.TASK_RESET_AIRFLOW_SETTINGS,
        provide_context=True,
        python_callable=reset_airflow_settings,
        op_kwargs={
            'parent_dag_name': context.get('task_instance').dag_id
        }
    )

    # trigger the RDS Update
    task_update_rds.execute(context=context)
    # trigger the airflow settings reset.
    task_reset_airflow_settings.execute(context=context)

