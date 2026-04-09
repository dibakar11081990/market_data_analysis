import boto3

class S3Helper:
    """
    A helper class for interacting with AWS S3.
    """

    def __init__(self, region_name='us-east-1'):
        self.s3 = boto3.client('s3', region_name=region_name)

    def delete_files_with_prefix(self, bucket_name, prefix):
        """
        Delete all files under a specific prefix in the S3 bucket.
        :param bucket_name: The name of the S3 bucket.
        :param prefix: The prefix path in the S3 bucket.
        """
        # check if ses object is present if not raise exception.
        if not self.s3:
            raise Exception('No S3 connection found')
        try:
            response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            if 'Contents' in response:
                for obj in response['Contents']:
                    self.s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
                print(f"Deleted all files with prefix '{prefix}' from bucket '{bucket_name}'.")
            else:
                print(f"No files found with prefix '{prefix}' in bucket '{bucket_name}'.")
        except Exception as e:
            print(f"Error deleting files with prefix '{prefix}': {e}")

    def delete_folder(self, bucket_name, folder_path):
        """
        Delete a folder and all its contents from the S3 bucket.
        :param bucket_name: The name of the S3 bucket.
        :param folder_path: The folder path in the S3 bucket ending with /.
        """
        try:
            print(f"Starting to delete folder '{folder_path}' in bucket '{bucket_name}'.")
            if not folder_path.endswith('/'):
                folder_path += '/'
            self.delete_files_with_prefix(bucket_name, folder_path)
            print(f"Successfully deleted folder '{folder_path}' in bucket '{bucket_name}'.")
        except Exception as e:
            print(f"Error deleting folder '{folder_path}': {e}")