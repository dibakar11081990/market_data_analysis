################################################################
"""
This subdag decides whether or not to delete the EMR cluster.
"""
# Imports
# Airflow Imports
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# Customs Functions and classes required for this workflow
from dags.common.py.aws.cloudformation import Cloudformation
from dags.common.py.airflow.tasks.dummy_tasks import AirflowDummyTaskCreator
# settings
from dags.mpm_adobe_revised.config import settings

################################################################

# Variables.

# Cloudformation Class object
cfn_obj = Cloudformation()


################################################################
# Create the Subdag.


def subdag_del_emr(parent_dag_name, child_dag_name, default_dag_args, ingestion_settings):
    """
    EMR Cluster deletion.

    :param parent_dag_name: Parent Dsg name
    :param child_dag_name: Child Dag Name
    :param default_dag_args: Default args coming from Parent
    :param ingestion_settings: User Settings for the DAG.
    :return: Subdag flow
    """

    # create the SubDAG object.
    sub_dag = DAG(dag_id="{}.{}".format(parent_dag_name, child_dag_name),
                  default_args=default_dag_args, schedule_interval=None)


    # get the EMR Settings
    adobe_emr_settings = ingestion_settings.get('emr', {})

    # check if EMR needs to be deleted if so delete it if not leave it running.
    if adobe_emr_settings.get('ShutdownCluster', True):

        # task to delete the emr cluster.
        task_delete_emr_cluster = PythonOperator(
            task_id=settings.TASK_DELETE_EMR_CLUSTER,
            python_callable=cfn_obj.delete_stack,
            op_kwargs={
                'name': settings.EMR_CLOUDFORMATION_STACK_NAME
            },
            dag=sub_dag
        )

    else:

        # Dummy task which says EMR Cluster will not be deleted.
        dummy_obj = AirflowDummyTaskCreator(dag=sub_dag)
        task_donot_delete_emr = dummy_obj.create(task_name='DONT_DELETE_EMR')


    # return the final dag.
    return sub_dag

################################################################
# End of EMR Deletion SubDag.
