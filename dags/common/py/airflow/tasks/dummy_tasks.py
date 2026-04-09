##################################################################
"""
This Python file contains all the DummyOperator tasks
"""
# Import the dummy operator.
from airflow.operators.dummy_operator import DummyOperator


##################################################################


class AirflowDummyTaskCreator(object):

    def __init__(self, dag):
        """
        Class constructor.
        """

        # dag object
        self.dag = dag


    def create(self, task_name):
        """
        start node
        :param task_name: Task id for the dummy operator.
        :return:
        """

        # create the dummy operator
        task = DummyOperator(
            task_id=task_name,
            dag=self.dag,
            on_failure_callback=None,
        )

        # return the task.
        return task


##################################################################
