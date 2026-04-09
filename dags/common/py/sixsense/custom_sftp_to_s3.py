from __future__ import annotations

from collections.abc import Sequence
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING
from urllib.parse import urlsplit
from dags.common.py.utils.verbose_log import verbose, log
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.ssh.hooks.ssh import SSHHook
import os
from datetime import datetime
import gc
from contextlib import contextmanager

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CustomSFTPToS3Operator(BaseOperator):
    """
    Transfer files from an SFTP server to Amazon S3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SFTPToS3Operator`

    :param sftp_conn_id: The sftp connection id. The name or identifier for
        establishing a connection to the SFTP server.
    :param sftp_path: The sftp remote path. This is the specified file path
        for downloading the file from the SFTP server.
    :param s3_conn_id: The s3 connection id. The name or identifier for
        establishing a connection to S3
    :param s3_bucket: The targeted s3 bucket. This is the S3 bucket to where
        the file is uploaded.
    :param s3_key: The targeted s3 key. This is the specified path for
        uploading the file to S3.
    :param use_temp_file: If True, copies file first to local,
        if False streams file from SFTP to S3.
    :param fail_on_file_not_exist: If True, operator fails when file does not exist,
        if False, operator will not fail and skips transfer. Default is True.
    """

    template_fields: Sequence[str] = ("s3_key", "sftp_path", "s3_bucket")

    def __init__(
        self,
        *,
        s3_bucket: str,
        s3_key: str,
        sftp_path: str,
        sftp_conn_id: str = "ssh_default",
        s3_conn_id: str = "aws_default",
        use_temp_file: bool = True,
        fail_on_file_not_exist: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.sftp_path = sftp_path
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_conn_id = s3_conn_id
        self.use_temp_file = use_temp_file
        self.fail_on_file_not_exist = fail_on_file_not_exist

    @staticmethod
    def get_s3_key(s3_key: str) -> str:
        """Parse the correct format for S3 keys regardless of how the S3 url is passed."""
        parsed_s3_key = urlsplit(s3_key)
        return parsed_s3_key.path.lstrip("/")

    @contextmanager
    def secure_temp_file(self):
        """Context manager that guarantees permanent file deletion"""
        temp_file = NamedTemporaryFile(delete=False)
        temp_file_path = temp_file.name
        temp_file.close()  # Close immediately to allow other processes to access
        
        try:
            yield temp_file_path
        finally:
            # Ensure permanent deletion
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)  # Permanent deletion
                log(f"File permanently deleted: {temp_file_path}")
            
            # Optional: Force garbage collection
            gc.collect()
            log(":::::::: Garbage collection started ::::::::")

    def execute(self, context: Context) -> None:
        self.s3_key = self.get_s3_key(self.s3_key)

        log("Creating ssh hook and s3 hook")
        ssh_hook = SSHHook(ssh_conn_id=self.sftp_conn_id)
        s3_hook = S3Hook(self.s3_conn_id)

        sftp_client = ssh_hook.get_conn().open_sftp()

        try:
            log("Details of file to be downloaded")
            sftp_client.stat(self.sftp_path)
        except FileNotFoundError:
            if self.fail_on_file_not_exist:
                raise
            log(f"File {self.sftp_path} not found on SFTP server. Skipping transfer.")
            return

        if self.use_temp_file:
            # Use the context manager for guaranteed cleanup
            with self.secure_temp_file() as temp_file_path:
                verbose(f"Downloading {self.sftp_path} in temporary file", "CRITICAL")
                
                try:
                    sftp_start_time = datetime.now()
                    sftp_client.get(self.sftp_path, temp_file_path)
                    sftp_end_time = datetime.now()
                    log(f"Downloaded temp file: {temp_file_path} in {sftp_end_time - sftp_start_time} of size {os.path.getsize(temp_file_path)/ (1024 ** 3)} GB")
                except Exception as e:
                    verbose(f"Error occurred during SFTP download: {e}", 'ERROR')
                    raise
                
                verbose(f"Loading {self.sftp_path} data to S3", "CRITICAL")
                try:
                    s3_upload_start_time = datetime.now()
                    s3_hook.load_file(filename=temp_file_path, key=self.s3_key, bucket_name=self.s3_bucket, replace=True)
                    s3_upload_end_time = datetime.now()
                    log(f"Uploaded temp file to S3 s3://{self.s3_bucket}/{self.s3_key} in {s3_upload_end_time - s3_upload_start_time}")
                except Exception as e:
                    verbose(f"Error occurred during S3 upload: {e}", 'ERROR')
                    raise
                
        else:
            verbose(f"Streaming {self.sftp_path} file to S3: s3://{self.s3_bucket}/{self.s3_key}", "CRITICAL")
            with sftp_client.file(self.sftp_path, mode="rb") as data:
                s3_hook.get_conn().upload_fileobj(data, self.s3_bucket, self.s3_key, Callback=self.log.info)


# from __future__ import annotations

# from collections.abc import Sequence
# from tempfile import NamedTemporaryFile
# from typing import TYPE_CHECKING
# from urllib.parse import urlsplit
# from dags.common.py.utils.verbose_log import verbose, log
# from airflow.models import BaseOperator
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.providers.ssh.hooks.ssh import SSHHook
# import os
# from datetime import datetime
# import gc
# from contextlib import contextmanager

# if TYPE_CHECKING:
#     from airflow.utils.context import Context


# class CustomSFTPToS3Operator(BaseOperator):
#     """
#     Transfer files from an SFTP server to Amazon S3.

#     .. seealso::
#         For more information on how to use this operator, take a look at the guide:
#         :ref:`howto/operator:SFTPToS3Operator`

#     :param sftp_conn_id: The sftp connection id. The name or identifier for
#         establishing a connection to the SFTP server.
#     :param sftp_path: The sftp remote path. This is the specified file path
#         for downloading the file from the SFTP server.
#     :param s3_conn_id: The s3 connection id. The name or identifier for
#         establishing a connection to S3
#     :param s3_bucket: The targeted s3 bucket. This is the S3 bucket to where
#         the file is uploaded.
#     :param s3_key: The targeted s3 key. This is the specified path for
#         uploading the file to S3.
#     :param use_temp_file: If True, copies file first to local,
#         if False streams file from SFTP to S3.
#     :param fail_on_file_not_exist: If True, operator fails when file does not exist,
#         if False, operator will not fail and skips transfer. Default is True.
#     """

#     template_fields: Sequence[str] = ("s3_key", "sftp_path", "s3_bucket")

#     def __init__(
#         self,
#         *,
#         s3_bucket: str,
#         s3_key: str,
#         sftp_path: str,
#         sftp_conn_id: str = "ssh_default",
#         s3_conn_id: str = "aws_default",
#         use_temp_file: bool = True,
#         fail_on_file_not_exist: bool = True,
#         **kwargs,
#     ) -> None:
#         super().__init__(**kwargs)
#         self.sftp_conn_id = sftp_conn_id
#         self.sftp_path = sftp_path
#         self.s3_bucket = s3_bucket
#         self.s3_key = s3_key
#         self.s3_conn_id = s3_conn_id
#         self.use_temp_file = use_temp_file
#         self.fail_on_file_not_exist = fail_on_file_not_exist

#     @staticmethod
#     def get_s3_key(s3_key: str) -> str:
#         """Parse the correct format for S3 keys regardless of how the S3 url is passed."""
#         parsed_s3_key = urlsplit(s3_key)
#         return parsed_s3_key.path.lstrip("/")

#     @contextmanager
#     def execute(self, context: Context) -> None:
#         self.s3_key = self.get_s3_key(self.s3_key)

#         log("Creating ssh hook and s3 hook")
#         ssh_hook = SSHHook(ssh_conn_id=self.sftp_conn_id)
#         s3_hook = S3Hook(self.s3_conn_id)

#         sftp_client = ssh_hook.get_conn().open_sftp()

#         try:
#             log("Details of file to be downloaded")
#             sftp_client.stat(self.sftp_path)
#         except FileNotFoundError:
#             if self.fail_on_file_not_exist:
#                 raise
#             log(f" File {self.sftp_path} not found on SFTP server. Skipping transfer. " )
#             return

#         if self.use_temp_file:
#             try:
#                 with NamedTemporaryFile("w", delete=False) as temp_file:
#                     verbose(f" Downloading {self.sftp_path} in temporary file", "CRITICAL")
#                     temp_file_path = temp_file.name
                    
#                     try:
#                         sftp_start_time = datetime.now()
#                         sftp_client.get(self.sftp_path, temp_file_path)
#                         sftp_end_time = datetime.now()
#                         log(f" Downloaded temp file: {temp_file_path} in {sftp_end_time - sftp_start_time} of size {os.path.getsize(temp_file_path)/ (1024 ** 3)} GB")
#                     except Exception as e:
#                         verbose(f"Error occurred: {e}", 'ERROR')
#                         raise
                    
#                     verbose(f" Loading {self.sftp_path} data to S3", "CRITICAL")
#                     try:
#                         s3_upload_start_time = datetime.now()
#                         s3_hook.load_file(filename=temp_file_path, key=self.s3_key, bucket_name=self.s3_bucket, replace=True)
#                         s3_upload_end_time = datetime.now()
#                         log(f" Uploaded temp file to S3 s3://{self.s3_bucket}/{self.s3_key} in {s3_upload_end_time - s3_upload_start_time}")
#                     except Exception as e:
#                         verbose(f"Error occurred: {e}", 'ERROR')
#                         raise
#             finally:
#                 if os.path.exists(temp_file_path):
#                     os.remove(temp_file_path)
#                     log(f"File permanently deleted: {temp_file_path}")
                
#                 log(":::::::: Garbage collection ::::::::")
#                 gc.collect()
                

#                 # # Clean up temp file
#                 # os.unlink(temp_file.name)
#                 # log(f" Removed temp file: {temp_file_path}" )
#         else:
#             verbose(f"Streaming {self.sftp_path} file to S3: s3://{self.s3_bucket}/{self.s3_key}", "CRITICAL")
#             with sftp_client.file(self.sftp_path, mode="rb") as data:
#                 s3_hook.get_conn().upload_fileobj(data, self.s3_bucket, self.s3_key, Callback=self.log.info)

