#############################################################################
"""
Email Alerting Function would be implemented here via AWS SNS
"""
#############################################################################
# Imports


# # Importing the base book
# from airflow.hooks.base_hook import BaseHook
# # Importing the Slack Webhook
# from airflow.contrib.operators.sns_publish_operator import SnsPublishOperator
# # importing the common settings
# from config.settings import AIRFLOW_AWS_CONN_ID, SNS_AIRFLOW_NOTIFICATION_TOPIC_ARN

from airflow.utils.email import send_email
from jinja2 import Template



import os
# Get the System Environment
ENVIRONMENT = os.environ['ENVIRONMENT']

#############################################################################


def task_fail_email_alert(context):
    """
    CallBack function which would send a email failure message to email.
    :param context:
    :return:
    """

    # prepare the message which needs to be send to slack
    
    email_msg = f"""
        DAG Failed: {context['dag'].dag_id}<br>
        Environment: {ENVIRONMENT}<br>
        Task: {context.get('task_instance').task_id}<br>
        DAG: {context.get('task_instance').dag_id}<br>
        Execution Time: {context.get('execution_date')}<br>
        Log Url: <a href='{context.get('task_instance').log_url}'>{context.get('task_instance').log_url}</a><br>
        """

    to = ['ese.esae.mdae.all@tredence_analytics.com']
    cc = []
    subject = f'DAG Failed: {context["dag"].dag_id}'

    send_email(to=to, cc=cc, subject=subject, html_content=email_msg)

