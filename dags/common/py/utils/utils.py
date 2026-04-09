##################################################################
"""
This Python file would contain common utility functions which
can be used across projects.
"""
# Imports
import json
# datetime libs
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, MONTHLY


##################################################################

def generate_month_iterator(start_date, end_date):
    """
    Given a start date and end date this function would return the start and end of every month
    :param start_date: start date of the iterator
    :param end_date:  end date of the iterator
    :return: [{'start_date':value, 'end_date': value}]
    """

    month_iter = [{'start_date': dt.strftime(format='%Y%m%d'),
                   'end_date': (dt + relativedelta(day=31)).strftime(format='%Y%m%d')}
                  for dt in rrule(MONTHLY,
                                  dtstart=datetime.strptime(start_date, '%Y%m%d').replace(day=1),
                                  until=datetime.strptime(end_date, '%Y%m%d'))]

    month_iter[0]['start_date'] = start_date
    month_iter[-1]['end_date'] = end_date

    # return the final date list.
    return month_iter



def jinja_template_formatter(parent_dag_name, child_dag_name, task_name, key=None):
    """
    Jinja templating and string.format() are incompatible
    this function provides a workaround

    :param parent_dag_name: Parent DAG ID
    :param child_dag_name: Subdag DAG ID
    :param task_name: task name from which the key needs to be retrieved
    :param key: key which needs to be retrieved from the task_id
    :return: jinja template
    """

    if key is None:

        jinja_template = "{{ task_instance.xcom_pull(dag_id='{0}.{1}', task_ids='{2}') }}".format(
            parent_dag_name, child_dag_name, task_name)

    else:

        jinja_template = "{{ task_instance.xcom_pull(dag_id='{0}.{1}', task_ids='{2}', key='{3}') }}".format(
            parent_dag_name, child_dag_name, task_name, key)

    return jinja_template.replace("{", "{{").replace("}", "}}")
