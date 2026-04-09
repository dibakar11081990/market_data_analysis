##################################################################
"""
A common script to start the EMR CLuster.
"""
# Imports

# import EMR class and its related functions
from dags.common.py.aws.emr import EMR
# import cloudformation class and its related functions.
from dags.common.py.aws.cloudformation import Cloudformation

##################################################################
# Class objects.


# EMR Class object.
emr_obj = EMR()

# Cloudformation Class object
cfn_obj = Cloudformation()




##################################################################

def get_emr_running_cluster(emr_cluster_name):
    """
    this function would be used to get all the EMR clusters which are currently in
    RUNNING/WAITING state filtered by cluster name
    :param emr_cluster_name: cluster name for filtering only valid clusters
    :return: running_clusters list.
    """

    # filter only those EMR clusters in RUNNING / WAITING State.
    emr_cluster_states = ['RUNNING', 'WAITING']

    # Check if the cluster is already running.
    # get the list of clusters running.
    emr_running_clusters = emr_obj.list_clusters(cluster_states=emr_cluster_states,
                                                 cluster_name=emr_cluster_name)

    # return the running clusters.
    return emr_running_clusters



def airflow_create_emr_cluster(emr_cluster_name, cfn_template_name, cfn_template_file,
                               cfn_params, predict_core_nodes=False):
    """
    This common function would be used to create the emr cluster.
    it checks if a cluster is already running.

    :param emr_cluster_name:  emr cluster name
    :param cfn_template_name: cloudformation template name
    :param cfn_template_file: emr cloudformation template file
    :param cfn_params: cloudformation template parameters
    :param predict_core_nodes: boolean which says if we are going to predict the core nodes.
    :return: emr cluster jobflow id
    """

    # get the running clusters
    emr_running_clusters = get_emr_running_cluster(emr_cluster_name=emr_cluster_name)

    # check if we need to estimate the cluster size
    if len(emr_running_clusters) == 1:

        # cluster already running. return the jobflowid.
        return emr_running_clusters[0]['Id']

    else:

        # Convert dict to cfn style parameters
        cfn_emr_parameters = [{'ParameterKey': key, 'ParameterValue': value}
                              for key, value in cfn_params.items()]

        # issue the request to create the emr cluster.
        cfn_obj.create_stack(name=cfn_template_name,
                             template=cfn_template_file,
                             parameters=cfn_emr_parameters)

        # get the running clusters.
        emr_running_clusters = get_emr_running_cluster(emr_cluster_name=emr_cluster_name)

        # return the cluster id.
        return emr_running_clusters[0]['Id']

##################################################################
# End of create emr cluster func.
