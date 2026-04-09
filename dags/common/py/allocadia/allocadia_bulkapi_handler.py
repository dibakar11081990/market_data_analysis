############################################################################
"""
This Functions in this  file performs the following
1) Issue the HTTP get request from the API provided by Allocadia
2) Download the Zip file on to the Matillion Instance
3) Unzip the files in the temp location
4) Upload the files to AWS S3 location

Bucket would be having the following structure
bsm-allocadiadata-public => dt_{timestamp} => files
"""
############################################################################
# Imports
import requests
from concurrent import futures
import zipfile
from io import BytesIO
import boto3
import os
import pandas as pd
import time
from io import StringIO
import json
from dags.common.py.utils.verbose_log import log


############################################################################


def allocadia_bulkapi_handler(logger, job_settings):
    """
    This function would be used for downloading the Allocadia data as a zip file on to AWS lambda temp location
    :param logger:  Logger object  coming from the driver
    :param job_settings: job setting coming from the matillion python component
    :return: nothing
    """

    # issue the Allocadia API request.
    response = requests.get(job_settings['ALLOCADIA_API_URL'],
                            auth=(job_settings['ALLOCADIA_USERNAME'],
                                  job_settings['ALLOCADIA_PASSWORD']))

    # check if the response is a success
    if response:

        # open the zip file location in binary mode
        zfile = open(job_settings['LAMBDA_ZIP_FILE_NAME'], 'wb')

        # write the response received from the API request
        zfile.write(response.content)

        # Close the file.
        zfile.close()

        log("get_allocadia_data() Allocadia API call successfully completed.")

        # return the success message
        return True

    else:
        log(f"get_allocadia_data() Allocadia API call Failed. Status Code: {response.status_code}, Response: {response.text}", "ERROR")
        return False


def upload(logger, job_settings, s3_conn, zipdata, filename, aws_object):
    """
    Thread Pool Workers would be using this function to send the file in parallel.
    :param logger:  logger object coming from the matillion python component
    :param job_settings: job settings coming from the matillion python component
    :param s3_conn: aws s3 connection
    :param zipdata: zip file object
    :param filename: file name which needs to be read from the zip file and sent to S3
    :param aws_object: aws s3 object path where the file needs to be placed
    :return: success/failure message, filename
    """

    # Set the default upload_status as success
    upload_status = 'success'
    # new file name format to have FY year in the file name
    filename_with_fy = filename.split('.')[0] + '_' + job_settings['FY'] + '.' + filename.split('.')[1]
    log(f'old file name {filename} with FY file name {filename_with_fy}')

    try:

        # read the file from the zip file and convert it in to a dataframe
        df = pd.read_csv(BytesIO(zipdata.read(filename)))
        # insert the ingestion timestamp
        df['ingestion_timestamp'] = int(round(time.time() * 1000))
        # insert new column to hold the FY
        df['FY'] = job_settings['FY']

        # if not df.empty:
        #     # insert the ingestion timestamp
        #     df['ingestion_timestamp'] = int(round(time.time() * 1000))
        #     # insert new column to hold the FY
        #     df['FY'] = job_settings['FY']

        # convert all the columns to lowercase
        df.columns = [column.lower() for column in df.columns]

        # get the columns from snowflake.
        snowflake_columns = job_settings['CSV_SNOWFLAKE_MAPPING'][filename.split(".")[0].upper()]['table_schema']
        # remove the ingestion_timestamp column
        snowflake_columns = [column for column in snowflake_columns if column != 'ingestion_timestamp']

        # buffer object
        csv_buffer = StringIO()

        # raise an exception whenever there is a schema mismatch.
        if not set(snowflake_columns).issubset(set(df.columns)) and job_settings['LATEST_FY'] == job_settings['FY'] :

            # save the dataframe in to the buffer space
            df.to_csv(csv_buffer, index=False)

            # save the csv file from the buffer in to s3 using boto3
            s3_conn.put_object(
                Bucket=job_settings['AWS_BUCKET'],
                Key=os.path.join(aws_object, filename),
                Body=csv_buffer.getvalue())

            raise Exception("Schema Mismatch between Snowflake and the CSV. File >>> {0}".format(filename.split(".")[0].upper()))

        # adjust columns ordering according to snowflake order
        df = df.reindex(columns=job_settings['CSV_SNOWFLAKE_MAPPING'][filename.split(".")[0].upper()]['table_schema'])

        # save the dataframe in to the buffer space
        df.to_csv(csv_buffer, index=False)


        # save the csv file from the buffer in to s3 using boto3
        s3_conn.put_object(
            Bucket=job_settings['AWS_BUCKET'],
            Key=os.path.join(aws_object, filename_with_fy),
            Body=csv_buffer.getvalue())

    except Exception as e:
        log(f"upload() function raised an exception while uploading the file >>>  {e}", "ERROR")
        # Change the upload status to failed.
        upload_status = 'fail'

    finally:
        # return the file name along with the upload status.
        return filename_with_fy, upload_status


def send_to_s3(logger, job_settings, aws_object):
    """
    This function would unzip the file received from the allocadia API and send it to AWS S3 Bucket location.
    :param logger: logger object coming from the matillion python component
    :param job_settings: job settings coming from the matillion python component
    :param aws_object: aws s3 location where the files downloaded from the allocadia needs to be uploaded
    :return: success or failure message
    """

    # zip file
    zipdata = zipfile.ZipFile(job_settings['LAMBDA_ZIP_FILE_NAME'])

    # get the connection to S3 using boto.
    s3_conn = boto3.client('s3',
                            aws_access_key_id = job_settings['AWS_ACCESS_KEY'],
                            aws_secret_access_key = job_settings['AWS_SECRET_KEY']
                        )
    
    log(f'before upload {json.dumps(job_settings, indent=4)}' )
    log(f'file_names {zipdata.namelist()}' )

    # use threadpool executor to upload the file to S3.
    # Worker Threads would be uploading files individually in parallel.
    with futures.ThreadPoolExecutor(max_workers=int(job_settings['THREADPOOL_EXECUTOR_WORKERS'])) as executor:
        # a list to hold the return messages.
        future_list = [executor.submit(upload,
                                       logger,
                                       job_settings,
                                       s3_conn,
                                       zipdata,
                                       filename,
                                       aws_object) for filename in zipdata.namelist()]

    log(f'future_list: {future_list}' )
    # update the result from each of the worker threads.
    result = {'success': [], 'fail': []}
    # loop through each of the thread result
    for future in future_list:
        # get the result of the particular thread
        filename, status = future.result()
        # update the result dictionary.
        result[status].append(filename)

    # Check if there are no failures
    if len(result['fail']) == 0:
        log("send_to_s3() Files Successfully uploaded to AWS S3.")
        return True

    else:
        log("send_to_s3() Some of the Threads failed while sending to AWS S3.", "ERROR")
        return False
