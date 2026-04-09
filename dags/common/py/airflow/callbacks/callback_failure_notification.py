#############################################################################
"""
A callback for handling notification failures.
"""
# import
from dags.common.py.notifications.slack_alert import task_fail_slack_alert
from dags.common.py.notifications.email_alert import task_fail_email_alert
import os

#############################################################################

ENVIRONMENT = os.environ['ENVIRONMENT']

# function
def callback_failure_notification(context):
    """
    A Common callback function which handles failure notifications to Slack / Email
    :param context:  DAG COntext
    :return:
    """

    # call the call back function for slack alerting
    task_fail_slack_alert(context=context)

    if ENVIRONMENT.upper() == 'PRD':
        # call the callback function for email alerting
        task_fail_email_alert(context=context)

#############################################################################
# End of function.
