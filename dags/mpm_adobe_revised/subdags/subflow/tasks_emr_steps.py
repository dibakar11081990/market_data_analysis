##################################################################
"""
Tasks which would be used to submit jobs to the EMR Cluster.
"""
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator

from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
# Importing the settings file.
from dags.mpm_adobe_revised.config import settings
from config.settings import AIRFLOW_AWS_CONN_ID

##################################################################

def emr_spark_submit(dag, task_id, job_flow_id, spark_job_name, spark_args):
    """
    The purpose of this function is to submit a spark job as a Step to EMR Cluster.
    :param dag: dag to which this task needs to be attached.
    :param task_id: task_id for this task.
    :param job_flow_id: EMR cluster id.
    :param spark_job_name: Spark job name which needs to be submitted.
    :param spark_args: Spark Arguments.
    :return:
    """

    # Spark Submit Configuration
    spark_submit_args = [
        "/usr/bin/spark-submit",
        "--master",
        "yarn",
        "--deploy-mode",
        "client",
        "--driver-memory",
        "{driver_memory}g".format(driver_memory=spark_args['driver_memory']),
        "--driver-cores",
        "{driver_cores}".format(driver_cores=spark_args['driver_cores']),
        "--conf",
        "spark.driver.memoryOverhead={driver_overhead}".format(driver_overhead=spark_args['driver_overhead']),
        "--num-executors",
        "{num_executors}".format(num_executors=spark_args['num_executors']),
        "--executor-memory",
        "{executor_memory}g".format(executor_memory=spark_args['executor_memory']),
        "--executor-cores",
        "{executor_cores}".format(executor_cores=spark_args['executor_cores']),
        "--conf",
        "spark.executor.memoryOverhead={executor_overhead}".format(executor_overhead=spark_args['executor_overhead']),
        "--conf",
        "spark.dynamicAllocation.enabled=false",
        "--packages",
        "org.apache.hadoop:hadoop-aws:2.7.1",
        "--py-files",
        "s3://{name}/mars_spark/jobs.zip".format(name=settings.EMR_ARTIFACT_S3_BUCKET),
        "s3://{name}/mars_spark/driver.py".format(name=settings.EMR_ARTIFACT_S3_BUCKET),
        "--job",
        "{job_name}".format(job_name=spark_job_name),
        "--job-args",
        "input_path={input_path}".format(input_path=spark_args['input_path']),
        "output_path={output_path}".format(output_path=spark_args['output_path']),
        "start_date={start_date}".format(start_date=spark_args['start_date']),
        "end_date={end_date}".format(end_date=spark_args['end_date']),
        "lookup_path={lookup_path}".format(lookup_path=settings.LOOKUPS.get(spark_job_name, ''))
    ]

    # EMR Step Parameters.
    emr_step_parms = [
        {
            'Name': task_id,
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': spark_submit_args,
            }
        }
    ]

    # Submit a Spark job to EMR.
    task = EmrAddStepsOperator(
        task_id=task_id,
        job_flow_id=job_flow_id,
        aws_conn_id=AIRFLOW_AWS_CONN_ID,
        steps=emr_step_parms,
        dag=dag
    )

    # return the task
    return task



def emr_step_sensor(dag, task_name, emr_jobflow_id, emr_step_id, poke_interval):
    """
    EMR Step Sensor to Poll the status of the submitted task to EMR.
    :param dag: Dag to which this task needs to be attached to.
    :param task_name: Name given for the task.
    :param emr_jobflow_id: EMR Job Flow ID.
    :param emr_step_id: EMR Step id.
    :param poke_interval: Poke_interval
    :return:
    """

    # EMR Step sensor to check the status of the submitted task.
    task = EmrStepSensor(
        task_id=task_name,
        job_flow_id=emr_jobflow_id,
        step_id=emr_step_id,
        aws_conn_id=AIRFLOW_AWS_CONN_ID,
        poke_interval=poke_interval,
        retry_exponential_backoff=True,
        retries=10,
        dag=dag
    )

    # return the task's object.
    return task
