#########################################################################
"""
Allocadia Driver program

This would be the Entry Point from the Matillion Python component
"""

#########################################################################
# Imports
from datetime import datetime
from dags.common.py.allocadia.allocadia_bulkapi_handler import allocadia_bulkapi_handler, send_to_s3
from dags.common.py.utils.verbose_log import log

###########################################################################
# Functions


def AllocadiaBulkapiDriver(logger, job_settings):
    """
    Main Entry point for Allocadia Data ingestion
    :param logger: logger object coming from the Matillion Python component
    :param job_settings: job related settings coming from the Matillion Python component
    :return: s3 location where the data was downloaded which needs to be passed to the downstream components
    """

    # Update the object level path
    #aws_object = "{prefix}/dt={timestamp}".format(prefix=job_settings['AWS_BUCKET_PREFIX'],
    #                                              timestamp=datetime.now().strftime("%Y%m%d%H%M%S"))
    # changed to use the same folder/with timestamp across the run
    aws_object = job_settings['AWS_BUCKET_PREFIX']
    log(f'aws_obj: {aws_object}')

    # Call the function which gets the allocadia data
    if allocadia_bulkapi_handler(logger=logger,
                                 job_settings=job_settings):

        log("AllocadiaDriver() Allocadia API >>> Completed.")

        # send the downloaded data to Amazon S3
        if send_to_s3(logger=logger,
                      job_settings=job_settings,
                      aws_object=aws_object):

            log("AllocadiaDriver() AWS Upload to S3 >>> Completed.")

            s3_location = 's3://{0}/{1}'.format(job_settings['AWS_BUCKET'], aws_object)

            # return the full s3 path where the data has been stored
            return s3_location

        # Raise the required exceptions for error handling.
        else:
            log("AllocadiaDriver() Worker Threads Failed to upload to S3 Bucket.", "CRITICAL")
            raise Exception("AllocadiaDriver() Worker Threads Failed to upload to S3 Bucket. "
                            "Allocadia API Succeeded Though.")
    else:
        log("AllocadiaDriver() Allocadia API Failure.", "CRITICAL")
        raise Exception('AllocadiaDriver() Allocadia API Failure.')
