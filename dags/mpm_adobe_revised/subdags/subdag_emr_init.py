################################################################
"""
This subdag performs various Initialization required for EMR Initialization.
"""

# Airflow Imports
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
# Customs Functions and classes required for this workflow
from dags.common.py.airflow.emr_cluster_start import airflow_create_emr_cluster
from dags.common.py.aws.cloudformation import Cloudformation
from dags.common.py.spark import spark_config_generator
from airflow.operators.dummy_operator import DummyOperator

# settings
from dags.mpm_adobe_revised.config import settings
# Python standard libs
import os

##################################################################
# Variables.

# Download Spark Code / Build the code and upload to S3. Remove this after Jenkins integration.
# script = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
#                       'common', 'bash', 'spark_repo_s3_upload.sh ')

# Cloudformation Class object
cfn_obj = Cloudformation()

# SparkConfigGenerator class object
obj = spark_config_generator.SparkConfigGenerator()

################################################################

# Create the Subdag.

def subdag_emr_init(parent_dag_name, child_dag_name, default_dag_args, ingestion_settings):
    """
    EMR Initialization/ Creation SUBDAG.

    :param parent_dag_name: Parent DAg name
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

    # Update the CloudFormation parameters based on settings.
    cfn_params = settings.EMR_CLOUDFORMATION_PARMS
    cfn_params['MasterInstanceType'] = adobe_emr_settings.get('MasterInstanceType', 'm5.dummy')
    cfn_params['CoreInstanceType'] = adobe_emr_settings.get('CoreInstanceType', 'm5.dummy')
    cfn_params['CoreNodeCount'] = str(adobe_emr_settings.get('CoreNodeCount', 0))
    cfn_params['MinCoreNodeCount'] = str(adobe_emr_settings.get('CoreNodeCount', 0))
    cfn_params['MaxCoreNodeCount'] = str(adobe_emr_settings.get('CoreNodeCount', 0))


    # create the emr cluster task.
    task_create_emr_cluster = PythonOperator(
        task_id=settings.TASK_CREATE_EMR_CLUSTER,
        python_callable=airflow_create_emr_cluster,
        op_kwargs={
            'emr_cluster_name': settings.EMR_CLUSTER_NAME,
            'cfn_template_name': settings.EMR_CLOUDFORMATION_STACK_NAME,
            'cfn_template_file': settings.EMR_CLOUDFORMATION_TEMPLATE_FILE,
            'cfn_params': cfn_params
        },
        dag=sub_dag,
    )


    # generate the Spark Configurations
    task_spark_config_generator = PythonOperator(
        task_id=settings.TASK_SPARK_CONFIG_GENERATOR,
        provide_context=True,
        python_callable=obj.generate_config,
        op_kwargs={
            'no_of_nodes': adobe_emr_settings.get('CoreNodeCount', 0),
            'instance_type': adobe_emr_settings.get('CoreInstanceType', 'm5.dummy')
        },
        dag=sub_dag
    )

    start = DummyOperator(task_id="start")

    start >> task_create_emr_cluster
    

    # return the sub_dag
    return sub_dag

################################################################
# End of EMR Init SubDAG.
