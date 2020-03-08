import boto3
import glob
import os

import biapp.core.logger.log as log
from biapp.settings.config import AWS_REGION

logger = log.setup_custom_logger(__name__)


class S3Operator:

    def __init__(self):

        self.client = self.create_s3_client()

    def create_s3_client(self):
        """
        Creates a client with the AWS credentials configured in the AWS CLI.
        """
        client = boto3.client('s3', region_name=AWS_REGION)
        logger.info('Client created')

        return client

    def create_bucket(self, bucket):
        """
        Creates an S3 bucket with the specified name in the AWS region
        specified in the application config files.
        """

        try:
            self.client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
            )
            logger.info(f'{bucket} created')

        except self.client.exceptions.BucketAlreadyOwnedByYou:
            logger.info(
                f"'{bucket}' already exists!"
            )

    def deploy_code(self, bucket):
        """
        Deploys the application code to the specified S3 bucket. When the
        EMR cluster is created, the code is copied from the S3 bucket to the
        local disk of the master node and executed there.

        Args:
            bucket (str):
                The name of the S3 bucket to deploy the application to.
        """
        directory = f'{os.path.join(os.getcwd())}{os.sep}'
        extensions = ('cfg', 'py')

        try:
            for ext in extensions:

                for root, subdirs, files in os.walk(directory):
                    files = glob.glob(os.path.join(root, f'*.{ext}'))

                    for f in files:
                        relative_path = root.replace(directory, '')
                        filename = os.path.basename(f)
                        s3_path = os.path.join(relative_path, filename)
                        s3_path = s3_path.replace(os.sep, '/')

                        self.client.upload_file(
                            os.path.join(root, f),
                            bucket,
                            s3_path,
                        )
                        logger.info(f'{s3_path} written to S3')

        except Exception as e:
            raise(e)

    def list_bucket(self, bucket, prefix=''):
        """
        Returns a list of all object keys in the specified S3 bucket with
        the specified prefix.

        Args:
            bucket (str):
                The name of the S3 bucket containing the objects to list.

            prefix (str):
                Object key prefix to filter the results by.

        Returns:
            list
        """
        response = self.client.list_objects_v2(
            Bucket=bucket,
            Delimiter='',
            EncodingType='url',
            MaxKeys=10,
            Prefix=prefix,
            FetchOwner=False,
            RequestPayer='requester'
        )

        if response:
            return response['Contents']
