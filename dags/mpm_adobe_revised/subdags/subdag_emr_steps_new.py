from dags.mpm_adobe_revised.config import settings
from dags.common.py.emr_utils import jinja_template_formatter
from airflow.models import Variable

XCOM_SPARK_DRIVER_CORES = 'SPARK_DRIVER_CORES'
XCOM_SPARK_DRIVER_MEMORY = 'SPARK_DRIVER_MEMORY' 
XCOM_SPARK_EXECUTOR_CORES = 'SPARK_EXECUTOR_CORES'
XCOM_SPARK_EXECUTOR_INSTANCES = 'SPARK_EXECUTOR_INSTANCES'
XCOM_SPARK_EXECUTOR_MEMORY = 'SPARK_EXECUTOR_MEMORY'
XCOM_SPARK_YARN_DRIVER_MEMORYOVERHEAD = 'SPARK_YARN_DRIVER_MEMORYOVERHEAD'
XCOM_SPARK_YARN_EXECUTOR_MEMORYOVERHEAD = 'SPARK_YARN_EXECUTOR_MEMORYOVERHEAD'


def emr_steps_sessionization(step):
    parent_dag_name = "Adobe_Intraday_Ingestion"
    ingestion_settings = Variable.get(settings.AIRFLOW_VAR_ADOBE_ONDEMAND, default_var={}, deserialize_json=True)
    adobe_ingestion_settings = ingestion_settings.get('ingestion', {})
    emr_jobflow_id=jinja_template_formatter(parent_dag_name, settings.SUBDAG_EMR_INIT,
                                                            settings.TASK_CREATE_EMR_CLUSTER)

    emr_spark_args = spark_args = {

        # spark driver args
        'driver_memory': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_DRIVER_MEMORY),
        'driver_cores': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_DRIVER_CORES),
        'driver_overhead': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_YARN_DRIVER_MEMORYOVERHEAD),

        # spark executor args
        'num_executors': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_EXECUTOR_INSTANCES),
        'executor_memory': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_EXECUTOR_MEMORY),
        'executor_cores': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_EXECUTOR_CORES),
        'executor_overhead': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_YARN_EXECUTOR_MEMORYOVERHEAD),

        # input / output path
        'input_path': settings.SPARK_S3_LANDING_ZONE_BUCKET,
        'output_path': settings.SPARK_S3_ADOBE_STACK_BUCKET

    }

    emr_task_name=settings.TASK_EMR_SESSIONIZATION
    spark_jobname=settings.SPARK_JOB_SESSIONIZATION
    spark_job_name = spark_jobname
    step_task_id = emr_task_name # + '_{start_date}_{end_date}'
    step_sensor_task_id = emr_task_name + '_SENSOR' #+ '_{start_date}_{end_date}_SENSOR'
    formatted_step_task_id = step_task_id#.format(start_date=marketo_ingestion_settings.get('start_date', 20150101), end_date=marketo_ingestion_settings.get('end_date', 20150101))
    formatted_step_sensor_task_id = step_sensor_task_id#.format(start_date=marketo_ingestion_settings.get('start_date', 20150101), end_date=marketo_ingestion_settings.get('end_date', 20150101))

    # update the default start date and end date for the spark job.
    emr_spark_args['start_date'] = adobe_ingestion_settings.get('start_date', 20150101)
    emr_spark_args['end_date'] = adobe_ingestion_settings.get('end_date', 20150101)

    spark_args = emr_spark_args

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
            'Name': formatted_step_task_id,
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': spark_submit_args,
            }
        }
    ]
    if step == 'SESSIONIZATION':
        return {    
            'task_id': formatted_step_task_id,
            'job_flow_id': emr_jobflow_id,
            'aws_conn_id': 'aws_default',
            'steps': emr_step_parms
        }
    else:
        return {
            'task_id': formatted_step_sensor_task_id,
            'job_flow_id': emr_jobflow_id,
            'step_id': "{{ task_instance.xcom_pull('" + settings.SUBDAG_EMR_STEPS + "." + formatted_step_task_id +
                                                       "', key='return_value')[0] }}",
            'aws_conn_id': 'aws_default',
            'poke_interval': 180,
            'retry_exponential_backoff': True,
            'retries': 2,
        }
    

def emr_steps_id_stack(step):
    parent_dag_name = "Adobe_Intraday_Ingestion"
    ingestion_settings = Variable.get(settings.AIRFLOW_VAR_ADOBE_ONDEMAND, default_var={}, deserialize_json=True)
    adobe_ingestion_settings = ingestion_settings.get('ingestion', {})
    emr_jobflow_id=jinja_template_formatter(parent_dag_name, settings.SUBDAG_EMR_INIT,
                                                            settings.TASK_CREATE_EMR_CLUSTER)

    emr_spark_args = {

        # spark driver args
        'driver_memory': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_DRIVER_MEMORY),
        'driver_cores': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_DRIVER_CORES),
        'driver_overhead': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_YARN_DRIVER_MEMORYOVERHEAD),

        # spark executor args
        'num_executors': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_EXECUTOR_INSTANCES),
        'executor_memory': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_EXECUTOR_MEMORY),
        'executor_cores': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_EXECUTOR_CORES),
        'executor_overhead': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_YARN_EXECUTOR_MEMORYOVERHEAD),

        # input / output path
        'input_path': settings.SPARK_S3_ADOBE_STACK_BUCKET.rstrip("/") + '/sessionized_ds',
        'output_path': settings.SPARK_S3_ADOBE_STACK_BUCKET

    }

    emr_task_name=settings.TASK_EMR_IDSTACK
    spark_jobname=settings.SPARK_JOB_ID_STACK
    spark_job_name = spark_jobname
    step_task_id = emr_task_name # + '_{start_date}_{end_date}'
    step_sensor_task_id = emr_task_name + '_SENSOR' #+ '_{start_date}_{end_date}_SENSOR'
    formatted_step_task_id = step_task_id#.format(start_date=marketo_ingestion_settings.get('start_date', 20150101), end_date=marketo_ingestion_settings.get('end_date', 20150101))
    formatted_step_sensor_task_id = step_sensor_task_id#.format(start_date=marketo_ingestion_settings.get('start_date', 20150101), end_date=marketo_ingestion_settings.get('end_date', 20150101))

    # update the default start date and end date for the spark job.
    emr_spark_args['start_date'] = adobe_ingestion_settings.get('start_date', 20150101)
    emr_spark_args['end_date'] = adobe_ingestion_settings.get('end_date', 20150101)

    spark_args = emr_spark_args

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
            'Name': formatted_step_task_id,
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': spark_submit_args,
            }
        }
    ]
    if step == 'ID_STACK':
        return {    
            'task_id': formatted_step_task_id,
            'job_flow_id': emr_jobflow_id,
            'aws_conn_id': 'aws_default',
            'steps': emr_step_parms
        }
    else:
        return {
            'task_id': formatted_step_sensor_task_id,
            'job_flow_id': emr_jobflow_id,
            'step_id': "{{ task_instance.xcom_pull('" + settings.SUBDAG_EMR_STEPS + "." + formatted_step_task_id +
                                                       "', key='return_value')[0] }}",
            'aws_conn_id': 'aws_default',
            'poke_interval': 70,
            'retry_exponential_backoff': True,
            'retries': 2,
        }



# Conversion Stack Workflow.
def emr_steps_adobe_conv_stack(step):
    parent_dag_name = "Adobe_Intraday_Ingestion"
    ingestion_settings = Variable.get(settings.AIRFLOW_VAR_ADOBE_ONDEMAND, default_var={}, deserialize_json=True)
    adobe_ingestion_settings = ingestion_settings.get('ingestion', {})
    emr_jobflow_id=jinja_template_formatter(parent_dag_name, settings.SUBDAG_EMR_INIT,
                                                            settings.TASK_CREATE_EMR_CLUSTER)

    emr_spark_args = {

        # spark driver args
        'driver_memory': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_DRIVER_MEMORY),
        'driver_cores': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_DRIVER_CORES),
        'driver_overhead': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_YARN_DRIVER_MEMORYOVERHEAD),

        # spark executor args
        'num_executors': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_EXECUTOR_INSTANCES),
        'executor_memory': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_EXECUTOR_MEMORY),
        'executor_cores': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_EXECUTOR_CORES),
        'executor_overhead': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_YARN_EXECUTOR_MEMORYOVERHEAD),

        # input / output path
        'input_path': settings.SPARK_S3_ADOBE_STACK_BUCKET.rstrip("/") + '/sessionized_ds',
        'output_path': settings.SPARK_S3_ADOBE_STACK_BUCKET

    }

    emr_task_name=settings.TASK_EMR_CONVERSION_STACK
    spark_jobname=settings.SPARK_JOB_CONV_STACK
    spark_job_name = spark_jobname
    step_task_id = emr_task_name # + '_{start_date}_{end_date}'
    step_sensor_task_id = emr_task_name + '_SENSOR' #+ '_{start_date}_{end_date}_SENSOR'
    formatted_step_task_id = step_task_id#.format(start_date=marketo_ingestion_settings.get('start_date', 20150101), end_date=marketo_ingestion_settings.get('end_date', 20150101))
    formatted_step_sensor_task_id = step_sensor_task_id#.format(start_date=marketo_ingestion_settings.get('start_date', 20150101), end_date=marketo_ingestion_settings.get('end_date', 20150101))

    # update the default start date and end date for the spark job.
    emr_spark_args['start_date'] = adobe_ingestion_settings.get('start_date', 20150101)
    emr_spark_args['end_date'] = adobe_ingestion_settings.get('end_date', 20150101)

    spark_args = emr_spark_args

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
            'Name': formatted_step_task_id,
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': spark_submit_args,
            }
        }
    ]
    if step == 'CONVERSION_STACK':
        return {    
            'task_id': formatted_step_task_id,
            'job_flow_id': emr_jobflow_id,
            'aws_conn_id': 'aws_default',
            'steps': emr_step_parms
        }
    else:
        return {
            'task_id': formatted_step_sensor_task_id,
            'job_flow_id': emr_jobflow_id,
            'step_id': "{{ task_instance.xcom_pull('" + settings.SUBDAG_EMR_STEPS + "." + formatted_step_task_id +
                                                       "', key='return_value')[0] }}",
            'aws_conn_id': 'aws_default',
            'poke_interval': 60,
            'retry_exponential_backoff': True,
            'retries': 2,
        }
    
# Engagement Stack Workflow.
def emr_steps_adobe_engagement_stack(step):
    parent_dag_name = "Adobe_Intraday_Ingestion"
    ingestion_settings = Variable.get(settings.AIRFLOW_VAR_ADOBE_ONDEMAND, default_var={}, deserialize_json=True)
    adobe_ingestion_settings = ingestion_settings.get('ingestion', {})
    emr_jobflow_id=jinja_template_formatter(parent_dag_name, settings.SUBDAG_EMR_INIT,
                                                            settings.TASK_CREATE_EMR_CLUSTER)

    emr_spark_args = {

        # spark driver args
        'driver_memory': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_DRIVER_MEMORY),
        'driver_cores': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_DRIVER_CORES),
        'driver_overhead': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_YARN_DRIVER_MEMORYOVERHEAD),

        # spark executor args
        'num_executors': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_EXECUTOR_INSTANCES),
        'executor_memory': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_EXECUTOR_MEMORY),
        'executor_cores': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_EXECUTOR_CORES),
        'executor_overhead': jinja_template_formatter(
            parent_dag_name, settings.SUBDAG_EMR_INIT,
            settings.TASK_SPARK_CONFIG_GENERATOR, XCOM_SPARK_YARN_EXECUTOR_MEMORYOVERHEAD),

        # input / output path
        'input_path': settings.SPARK_S3_ADOBE_STACK_BUCKET.rstrip("/") + '/sessionized_ds',
        'output_path': settings.SPARK_S3_ADOBE_STACK_BUCKET

    }

    emr_task_name=settings.TASK_EMR_ENGAGEMENT_STACK
    spark_jobname=settings.SPARK_JOB_EVT_STACK
    spark_job_name = spark_jobname
    step_task_id = emr_task_name # + '_{start_date}_{end_date}'
    step_sensor_task_id = emr_task_name + '_SENSOR' #+ '_{start_date}_{end_date}_SENSOR'
    formatted_step_task_id = step_task_id#.format(start_date=marketo_ingestion_settings.get('start_date', 20150101), end_date=marketo_ingestion_settings.get('end_date', 20150101))
    formatted_step_sensor_task_id = step_sensor_task_id#.format(start_date=marketo_ingestion_settings.get('start_date', 20150101), end_date=marketo_ingestion_settings.get('end_date', 20150101))

    # update the default start date and end date for the spark job.
    emr_spark_args['start_date'] = adobe_ingestion_settings.get('start_date', 20150101)
    emr_spark_args['end_date'] = adobe_ingestion_settings.get('end_date', 20150101)

    spark_args = emr_spark_args

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
            'Name': formatted_step_task_id,
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': spark_submit_args,
            }
        }
    ]
    if step == 'ENGAGEMENT_STACK':
        return {    
            'task_id': formatted_step_task_id,
            'job_flow_id': emr_jobflow_id,
            'aws_conn_id': 'aws_default',
            'steps': emr_step_parms
        }
    else:
        return {
            'task_id': formatted_step_sensor_task_id,
            'job_flow_id': emr_jobflow_id,
            'step_id': "{{ task_instance.xcom_pull('" + settings.SUBDAG_EMR_STEPS + "." + formatted_step_task_id +
                                                       "', key='return_value')[0] }}",
            'aws_conn_id': 'aws_default',
            'poke_interval': 120,
            'retry_exponential_backoff': True,
            'retries': 2,
        }