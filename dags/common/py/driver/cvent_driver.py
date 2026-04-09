##############################################################
"""
CVENT DRIVER PROGRAM
ENTRY POINT FROM MATILLION
"""
##############################################################
# Imports
# Boto for interacting with S3
import boto3
# Threadpool
from concurrent import futures
# CVENT API Handler class
from cvent.cvent_conference_api_handler import CventConferenceAPIHandler


# Global vars
# get the connection to S3 using boto.
s3 = boto3.client('s3')


################################################################

def create_cvent_threadpool(logger, job_settings):
    """
    This function spawns different threads which independently would call different API and would create
    files on S3

    Threads are choosen for now as its lightweight. Need to check if multiprocessing works via matillion.

    :param logger: logger object for logging
    :param job_settings: job settings coming from the matillion python component
    :return: status, s3_location in case of success
    """

    # Create the Cvent conference API handler object
    cvent_api_handler_class = CventConferenceAPIHandler(logger=logger,
                                                        s3_conn=s3,
                                                        job_settings=job_settings)

    # Worker Threads would be issuing the API request / uploading files individually in parallel.
    with futures.ThreadPoolExecutor(max_workers=int(job_settings['THREADPOOL_EXECUTOR_WORKERS'])) as executor:
        # Dynamically call the right function based on API.
        future_list = [
            executor.submit(
                getattr(cvent_api_handler_class,
                        'handle_{api_type}'.format(api_type=api_type.lower()) if
                        job_settings['API_SETTINGS'][api_type]['custom_api_handling'] else
                        'api_handling_routine'),
                api_type,
                job_settings['API_SETTINGS'][api_type])
            for api_type in job_settings['API_SETTINGS'].keys()
        ]

    # update the result from each of the worker threads.
    result = {'success': [], 'fail': []}

    # loop through each of the thread result
    for future in future_list:
        # get the result of the particular thread
        api_type, status = future.result()
        # update the result dictionary.
        result[status].append(api_type)

    # Check if there are no failures
    if len(result['fail']) == 0:
        logger.info("create_cvent_threadpool() API-Call/S3-Upload completed Successfully.")
        return True, cvent_api_handler_class.aws_object
    else:
        logger.critical("create_cvent_threadpool() API-Call/S3-Upload failed on a few threads.")
        return False, 'ThreadPool Failure.'



def CventConferenceDriver(logger, job_settings):
    """
    Entry point for cvent Conference in Matillion
    :param logger: Logger object for logging
    :param job_settings: job settings coming from Matillion
    :return: s3_location where the CSV files have been uploaded
    """

    # create the thread pool and issue the API request via threads
    status, aws_object = create_cvent_threadpool(logger, job_settings)

    # check if status was a success
    if status:

        logger.info("matillion_handler() ALL API's S3 Upload Completed successfully")

        # create the s3_location variable
        s3_location = 's3://{0}/{1}'.format(job_settings['AWS_BUCKET'], aws_object)

        # return the s3 location for matillion to pass on to the next component
        return s3_location

    else:
        logger.critical('matillion_handler() ThreadPool Failed, An error occurred.')
        raise Exception('matillion_handler() ThreadPool Failed, An error occurred.')
