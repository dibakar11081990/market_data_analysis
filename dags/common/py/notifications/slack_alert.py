#############################################################################
"""
Slack Alerting Function would be implemented here.
"""
#############################################################################
# Imports

# Importing the base book
from airflow.hooks.base_hook import BaseHook
# Importing the Slack Webhook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
# importing the common settings
from config.settings import AIRFLOW_SLACK_CONN_ID
import os
# Get the System Environment
ENVIRONMENT = os.environ['ENVIRONMENT']

#############################################################################


def task_fail_slack_alert(context):
    """
    This callback function would be called if any of the tasks fail in airflow DAG
    which would inturn send a message to a slack channel.
    :param context:
    :return:
    """

    # Create the slack connection and get the web token
    slack_web_hook_token = BaseHook.get_connection(AIRFLOW_SLACK_CONN_ID).password

    # prepare the message which needs to be send to slack
    slack_msg = """
            :red_circle: {dag} Workflow Failed at task {task}
            *Environment*: {env}
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
            env=ENVIRONMENT,
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )

    # Send the actual message to Slack.
    failed_alert = SlackWebhookOperator(
        task_id='SLACK_FAILED_ALERT',
        slack_webhook_conn_id=AIRFLOW_SLACK_CONN_ID,
        message=slack_msg)

    # Execute the Slack WebHook Dag
    return failed_alert.execute(context=context)


def task_success_slack_alert(dag):
    """
    Send Slack Success messages.
    :param dag: Dag to which this task needs to be attached to.
    :return:
    """

    # Create the slack connection and get the web token
    slack_web_hook_token = BaseHook.get_connection(AIRFLOW_SLACK_CONN_ID).password

    # prepare the message which needs to be send to slack
    slack_msg = """
                :green_circle: {{ task_instance.dag_id }} Workflow completed successfully.
                *Environment*: """+ENVIRONMENT+"""
                *Task*: {{ task_instance.task_id }} 
                *Dag*: {{ task_instance.dag_id }}
                *Execution Time*: {{ execution_date }} 
                *Log Url*: {{ task_instance.log_url }}
                """

    # Send the actual message to Slack.
    task_success_alert = SlackWebhookOperator(
        task_id='SLACK_SUCCESS_ALERT',
        slack_webhook_conn_id=AIRFLOW_SLACK_CONN_ID,
        trigger_rule='all_success',
        retries=3,
        message=slack_msg,
        dag=dag)

    # Execute the Slack WebHook Dag
    return task_success_alert
