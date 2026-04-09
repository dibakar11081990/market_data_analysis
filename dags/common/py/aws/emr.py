#############################################################################
"""
A Common Class for handling the AWS EMR API's
"""
#############################################################################

# Imports
import boto3
from dags.activity_marketo_v2_driver.utility_functions import aws_conn_credentials

#############################################################################

class EMRException(Exception):
    """
    Exception handling class.
    """
    pass


class EMR(object):
    """
    Central Class for Handling the BOTO3 EMR API.
    """

    # Valid EMR Status list
    VALID_EMR_STATUS = ['STARTING',
                            'BOOTSTRAPPING',
                            'RUNNING',
                            'WAITING',
                            'TERMINATING',
                            'TERMINATED',
                            'TERMINATED_WITH_ERRORS']

    # Our Default region in aws is US-EAST1
    DEFAULT_REGION = 'us-east-1'

    def __init__(self, region=None):
        """
        :param region: AWS region
        :type region: str
        """
        ACCESS_KEY, SECRET_KEY = aws_conn_credentials()
        # create a client to EMR.
        self.emr_client = boto3.client('emr', region_name=region or EMR.DEFAULT_REGION, aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY)

    def list_clusters(self, cluster_states, cluster_name, **kwargs):
        """
        List the clusters in EMR in particular state

        :param cluster_states: Valid list of states a cluster should be in
        :param cluster_name: Name for filtering the cluster
        :param kwargs: airflow context if passed
        :return: cluster information
        """

        try:
            response = self.emr_client.list_clusters(
                ClusterStates=cluster_states
            )

            # check if the above method returned a response
            if response:

                # Get all the clusters returned.
                clusters = response['Clusters']

                # filter those clusters according to the name
                clusters = [cluster for cluster in clusters if cluster['Name'] == cluster_name]


                # Filter only running clusters
                running_clusters = [cluster for cluster in clusters if cluster['Status']['State'] in
                                    ['RUNNING', 'WAITING']]

                # check how many clusters are returned
                if len(running_clusters) > 1:
                    raise EMRException("ALARM >>> More than one EMR cluster in running state.")

                # check if airflow context is being passed
                if 'ti' in kwargs and len(running_clusters) == 1:
                    # Push the job flow id as an Xcom variable so that we can submit jobs to EMR Cluster.
                    kwargs['ti'].xcom_push(key='JOB_FLOW_ID', value=running_clusters[0]['Id'])

                # return the cluster list
                return running_clusters

        except Exception as ex:
            raise EMRException('EMR list_clusters() Exception ErrorMessage >>> '+str(ex))