###############################################################
"""
Spark Config Generator
THis class makes sure that the entire cluster would be used by every Job.
"""
###############################################################
# Imports
import pandas as pd
import math
import json
from config.settings import EMR_INSTANCE_TYPES, XCOM_SPARK_DRIVER_CORES, XCOM_SPARK_DRIVER_MEMORY, \
    XCOM_SPARK_EXECUTOR_CORES, XCOM_SPARK_EXECUTOR_INSTANCES, XCOM_SPARK_EXECUTOR_MEMORY, \
    XCOM_SPARK_YARN_DRIVER_MEMORYOVERHEAD , XCOM_SPARK_YARN_EXECUTOR_MEMORYOVERHEAD


###############################################################


class SparkConfigGenerator(object):
    """
     Spark Class which would generate the right configuration
    """

    # Constants

    # Max Executor
    max_executor_instances = 192

    # Spark Memory overhead coefficient
    memory_overhead_coefficient = 0.1

    # Executor memory upper bound
    executor_memory_upper_bound = 64

    # Executor core upper bound
    executor_core_upper_bound = 5

    # How much of cores to be reserved for the operating system
    os_reserved_cores = 0

    # How much of memory to be reserved by the operating system
    os_reserved_memory = 0

    # Parallelism per core
    parallelism_per_core = 4

    def __init__(self):
        """
        Dummy Constructor class
        """

        pass

    def generate_config(self, no_of_nodes, instance_type, **kwargs):
        """
        The actual function which would generate the Config
        :param self:
        :param no_of_nodes: Pass in the no of nodes being started in the cluster. THis should be coming from the
                        Cloudformation template.
        :param instance_type: Ec2 Instance type being created.
        :param kwargs:
        :return:
        """

        if isinstance(no_of_nodes, str):
            no_of_nodes = int(no_of_nodes)

        # calculate the available memory per node.
        available_memory = (EMR_INSTANCE_TYPES[instance_type]['YARN_NODEMANAGER_MEMORY'] / 1024) - \
                           SparkConfigGenerator.os_reserved_memory

        # Calculate the available cores per node.
        available_cores = EMR_INSTANCE_TYPES[instance_type]['NO_OF_CORES'] - \
                          SparkConfigGenerator.os_reserved_cores

        print("SparkConfigGenerator no_of_nodes >>> " + str(no_of_nodes))
        print("SparkConfigGenerator instance_type >>> " + str(instance_type))
        print("SparkConfigGenerator available_memory >>> " + str(available_memory))
        print("SparkConfigGenerator available_cores >>> " + str(available_cores))

        # Final list containing the required rows
        final_data = []

        # Depending on the executor instances calculate the various os metrics
        for num in range(1, SparkConfigGenerator.max_executor_instances):
            # dict to hold each row
            t_dict = {'executor_per_node': num}

            # Total memory per executor
            t_dict['total_memory_per_executor'] = int(available_memory / num)

            # Memory overhead per executor
            t_dict['overhead_memory_per_executor'] = math.ceil(t_dict['total_memory_per_executor'] *
                                                               SparkConfigGenerator.memory_overhead_coefficient)

            # Memory per executor
            t_dict['memory_per_executor'] = t_dict['total_memory_per_executor'] - t_dict['overhead_memory_per_executor']

            # cores per executor
            t_dict['cores_per_executor'] = int(available_cores / num)

            # Unused memory per node
            t_dict['unused_memory_per_node'] = available_memory - (num * t_dict['total_memory_per_executor'])

            # Unused cores per node
            t_dict['unused_cores_per_node'] = available_cores - (num * t_dict['cores_per_executor'])

            # Append the Dict
            final_data.append(t_dict)

        # Create the dataframe using the rows being generated from the above loop.
        df = pd.DataFrame(final_data)

        print("\n" + df.head(100).to_string())

        # rearranging the columns
        cols = ['executor_per_node', 'total_memory_per_executor', 'overhead_memory_per_executor',
                'memory_per_executor', 'cores_per_executor', 'unused_memory_per_node', 'unused_cores_per_node']
        df = df[cols]

        # Filtering some of the rows from the dataframe  executor_memory_upper_bound --> 64 and cores_per_executor -->5
        df = df[(df['total_memory_per_executor'] <= SparkConfigGenerator.executor_memory_upper_bound) &
                (df['cores_per_executor'] <= SparkConfigGenerator.executor_core_upper_bound)]

        # Get the first row from the dataframe that would contain the required parms for spark submit
        executor_row_dict = df.iloc[0]

        print("\n Selected Executor Parameters")
        print(executor_row_dict)

        # final value for executors per node
        selected_executor_per_node = executor_row_dict['executor_per_node']

        # final value for spark executor instances
        spark_executor_instances = max(1, (no_of_nodes * selected_executor_per_node) - 1)

        # final value for Memory Overhead
        spark_yarn_executor_memoryoverhead = executor_row_dict['overhead_memory_per_executor'] * 1024 * 4

        # final value for executor memory
        spark_executor_memory = executor_row_dict['memory_per_executor']

        # final value for memory overhead
        spark_yarn_driver_memoryoverhead = executor_row_dict['overhead_memory_per_executor'] * 1024 * 4

        # Driver Memory
        spark_driver_memory = executor_row_dict['memory_per_executor']

        # spark.executor.cores
        spark_executor_cores = executor_row_dict['cores_per_executor']

        # spark.driver.cores
        spark_driver_cores = spark_executor_cores



        if no_of_nodes == 1 and spark_driver_cores == spark_driver_cores:
            spark_driver_cores = spark_driver_cores / 2
            spark_executor_cores = spark_executor_cores / 2
            spark_executor_memory = spark_executor_memory / 2
            spark_driver_memory = spark_driver_memory / 2

        # spark.default.parallelism
        spark_default_parallelism = self.parallelism_per_core * spark_executor_cores * spark_executor_instances
        spark_driver_cores = 3
        spark_executor_cores = 4
        spark_executor_memory = 19

        spark_config_dict = {
            XCOM_SPARK_EXECUTOR_INSTANCES: int(spark_executor_instances),
            XCOM_SPARK_YARN_EXECUTOR_MEMORYOVERHEAD: int(spark_yarn_executor_memoryoverhead),
            XCOM_SPARK_EXECUTOR_MEMORY: int(spark_executor_memory),
            XCOM_SPARK_YARN_DRIVER_MEMORYOVERHEAD: int(spark_yarn_driver_memoryoverhead),
            XCOM_SPARK_DRIVER_MEMORY: int(spark_driver_memory),
            XCOM_SPARK_EXECUTOR_CORES: int(spark_executor_cores),
            XCOM_SPARK_DRIVER_CORES: int(spark_driver_cores),
        }

        print("SparkConfigGenerator Printing the Final Selected parameters which would be used for spark-submit")
        print(json.dumps(spark_config_dict, indent=3))

        # Push spark configuration into XCom
        for key, value in spark_config_dict.items():
            kwargs['ti'].xcom_push(key=key, value=value)

        # return the final selected configuration
        return spark_config_dict

###############################################################
