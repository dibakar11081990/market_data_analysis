################################################################
"""
This python file would be used to create the DAG flow for sessionization/conversion/Engagement task
"""
# Imports
# util functions
from dags.common.py.utils.utils import generate_month_iterator
from dags.mpm_adobe_revised.config import settings


################################################################
# Sub flow for Adobe Stacks.


def flow_adobe_stacks(dag, task_start, task_end, emr_jobflow_id, emr_spark_args,
                      emr_task_name, spark_jobname, skip_vault, ingestion_settings):
    """
    Sessionization/ Engagement Stack / Purchase stack DAG flow.
    :param dag: dag to which the tasks created by this function should be attached to.
    :param task_start: task to start from
    :param task_end: final merging task
    :param emr_jobflow_id: emr cluster id
    :param emr_spark_args: emr spark parameters
    :param emr_task_name: Task name in EMR and in Airflow.
    :param spark_jobname: Spark job which needs to be run.
    :param skip_vault: this vault is used to skip this entire flow.
    :param ingestion_settings: Ingestion settings coming from Airflow Variables.
    :return:
    """

    poke_interval = 180 if spark_jobname == settings.SPARK_JOB_SESSIONIZATION else 120 \
        if spark_jobname == settings.SPARK_JOB_EVT_STACK else 60


    # vault to skip this part of the workflow.
    if not skip_vault:

        # task id creation.
        step_task_id = emr_task_name + '_{start_date}_{end_date}'
        step_sensor_task_id = emr_task_name + '_{start_date}_{end_date}_SENSOR'
        formatted_step_task_id = step_task_id.format(
            start_date=ingestion_settings.get('start_date', 20150101),
            end_date=ingestion_settings.get('end_date', 20150101))
        formatted_step_sensor_task_id = step_sensor_task_id.format(
            start_date=ingestion_settings.get('start_date', 20150101),
            end_date=ingestion_settings.get('end_date', 20150101))

        # update the default start date and end date for the spark job.
        emr_spark_args['start_date'] = ingestion_settings.get('start_date', 20150101)
        emr_spark_args['end_date'] = ingestion_settings.get('end_date', 20150101)

        # check if we need to split the job month wise.
        if ingestion_settings.get('split_job_bymonth', False):

            # get the month iterators.
            month_iter = generate_month_iterator(start_date=str(ingestion_settings.get('start_date', 20150101)),
                                                 end_date=str(ingestion_settings.get('end_date', 20150101)))

            # loop through each of the month and submit the spark job.
            for month_dict in month_iter:
                # update the start date and end date for the spark job.
                emr_spark_args['start_date'] = month_dict['start_date']
                emr_spark_args['end_date'] = month_dict['end_date']

                # format the task id
                formatted_step_task_id = step_task_id.format(start_date=month_dict['start_date'],
                                                             end_date=month_dict['end_date'])
                formatted_step_sensor_task_id = step_sensor_task_id.format(start_date=month_dict['start_date'],
                                                                           end_date=month_dict['end_date'])

                # submit the spark job.
                step_task = emr_spark_submit(dag=dag,
                                             task_id=formatted_step_task_id,
                                             job_flow_id=emr_jobflow_id,
                                             spark_job_name=spark_jobname,
                                             spark_args=emr_spark_args)

                # emr step sensor task.
                step_sensor_task = emr_step_sensor(dag=dag,
                                                   task_name=formatted_step_sensor_task_id,
                                                   emr_jobflow_id=emr_jobflow_id,
                                                   emr_step_id="{{ task_instance.xcom_pull('" + formatted_step_task_id +
                                                               "', key='return_value')[0]}}",
                                                   poke_interval=poke_interval)

                # Airflow Dag flow.
                task_start.set_downstream(step_task)
                step_task.set_downstream(step_sensor_task)
                step_sensor_task.set_downstream(task_end)

        else:

            # submit the spark job.
            step_task = emr_spark_submit(dag=dag,
                                         task_id=formatted_step_task_id,
                                         job_flow_id=emr_jobflow_id,
                                         spark_job_name=spark_jobname,
                                         spark_args=emr_spark_args)

            # emr step sensor task.
            step_sensor_task = emr_step_sensor(dag=dag,
                                               task_name=formatted_step_sensor_task_id,
                                               emr_jobflow_id=emr_jobflow_id,
                                               emr_step_id="{{ task_instance.xcom_pull('" + formatted_step_task_id +
                                                           "', key='return_value')[0] }}",
                                               poke_interval=poke_interval)

            # Airflow Dag flow.
            task_start.set_downstream(step_task)
            step_task.set_downstream(step_sensor_task)
            step_sensor_task.set_downstream(task_end)

    else:

        # just connect the start to end.
        task_start.set_downstream(task_end)

    return

################################################################
# End of Sessionization / Conversion stack / Engagement stack DAG Flow.
