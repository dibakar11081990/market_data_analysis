################################################################
"""
This subdag performs various Cleanup required for Adobe Automated Ingestion.
"""
# Imports.
# Airflow Settings
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
# Custom functions and Settings
from dags.mpm_adobe_revised.config import settings
from airflow.providers.http.operators.http import HttpOperator
import json

################################################################
# Functions

def reset_airflow_settings(parent_dag_name, **kwargs):
    """
    Reset the airflow settings back to the original values which were before.
    :param parent_dag_name: Parent DAG Name
    :param kwargs: Context being passed by the DAG
    :return:
    """

    print('{}.{}'.format(parent_dag_name, settings.SUBDAG_AUTOMATED_INGESTION_INIT))

    # get the original settings being set via the UI
    original_settings = kwargs['ti'].xcom_pull(dag_id='{}.{}'.format(
        parent_dag_name, settings.SUBDAG_AUTOMATED_INGESTION_INIT),
        task_ids=settings.TASK_SET_AIRFLOW_SETTINGS,
        key=settings.XCOM_ORIGINAL_INGESTION_SETTINGS)


    # prints.
    print("reset_airflow_settings() Ingestion_Settings received from XCOM is >>> " + str(original_settings))

    if original_settings is not None:
        # reset back the Ingestion / EMR Settings.
        Variable.set(settings.AIRFLOW_VAR_ADOBE_AUTOMATED, original_settings)
    else:
        # dummy default
        Variable.set(settings.AIRFLOW_VAR_ADOBE_AUTOMATED,
                     json.dumps({'emr': {"MasterInstanceType": "m5a.xlarge", "CoreInstanceType": "m5a.2xlarge"},
                                 'ingestion': {}}))


################################################################

# Create the Subdag.

def subdag_automated_cleanup(parent_dag_name, child_dag_name, args):
    """
    Automated Ingestion Cleanup task.

    :param parent_dag_name: Parent DAg name
    :param child_dag_name: Child Dag Name
    :param args: Default args coming from Parent
    :return: Subdag flow
    """

    # create the SubDAG object.
    sub_dag = DAG(dag_id="{}.{}".format(parent_dag_name, child_dag_name),
                  default_args=args, schedule_interval=None)

    # runid jinja template.
    runid_jinja_template = "{{ var.json.{var}.ingestion.get('job_run_id', 1) }}".format(var=settings.AIRFLOW_VAR_ADOBE_AUTOMATED).\
        replace("{", "{{").replace("}", "}}")

    # Task to Update the RDS Stating the job has finished via the api call.
    task_update_rds = HttpOperator(
        task_id=settings.TASK_UPDATE_RDS,
        endpoint='job/adp_update?runid=' + runid_jinja_template + '&mars_process_status=PROCESSING_SUCCESS',
        response_check=lambda response: response.status_code == 200,
        dag=sub_dag,
    )

    # Task to reset the airflow settings.
    task_reset_airflow_settings = PythonOperator(
        task_id=settings.TASK_RESET_AIRFLOW_SETTINGS,
        provide_context=True,
        python_callable=reset_airflow_settings,
        op_kwargs={
            'parent_dag_name': parent_dag_name
        },
        dag=sub_dag
    )

    # Subdag Flow.
    task_update_rds >> task_reset_airflow_settings

    return sub_dag

################################################################
# End of Automated Ingestion cleanup DAG.
