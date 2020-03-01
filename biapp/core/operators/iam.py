import boto3
import json

from biapp.core.logger import log
from biapp.settings.aws_policies import (
    EMR_SERVICE,
    EMR_TRUST_RELATIONSHIP,
    S3_FULL_ACCESS,
)
from biapp.settings.config import (
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_ROLE,
    AWS_SECRET_ACCESS_KEY,
)

logger = log.setup_custom_logger(__name__)


class IAMOperator:

    def __init__(self):

        self._dwh_role_arn = None
        self._dwh_role_id = None
        self.aws_role_policies = [EMR_SERVICE, S3_FULL_ACCESS]
        self.dwh_db_role = AWS_ROLE
        self.dwh_trust_policy = EMR_TRUST_RELATIONSHIP
        self.client = self.create_iam_client()

    @property
    def user_info(self):

        return self.client.get_user()

    @property
    def dwh_role_arn(self):

        return self._dwh_role_arn

    def create_iam_client(self):
        """
        Creates an IAM client with the AWS credentials in the application's
        config files. The AWS credentials must be of a user with admin access
        or with the IAMFullAccess and AmazonRedshiftFullAccess applied.

        Returns:
            boto3.client
        """
        client = boto3.client(
            service_name='iam',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

        logger.info('Client created')

        return client

    def create_role(self):
        """
        Creates an AWS role so that the Redshift client can load data from S3.
        The trust policy of the data warehouse role is declared in the config
        files and assigned as a property of this class.

        Returns:
            json
        """
        dwh_roles = self.client.list_roles()
        role_list = {x['RoleName'] for x in dwh_roles['Roles']}

        if self.dwh_db_role in role_list:
            self.detach_role_policies()
            self.delete_dwh_role()

        try:
            response = self.client.create_role(
                Path='/',
                RoleName=self.dwh_db_role,
                AssumeRolePolicyDocument=json.dumps(self.dwh_trust_policy),
                Description='Allows EMR to call AWS services on your behalf',
            )
        except self.client.exceptions.EntityAlreadyExistsException:
            logger.info(
                f"'{self.dwh_db_role}' already exists!"
            )

        self._dwh_role_id = response['Role']['RoleId']
        self._dwh_role_arn = response['Role']['Arn']

        logger.info(f"'{self.dwh_db_role}' created")

        return response

    def attach_role_policies(self):
        """
        Attaches a list of policies to the AWS role created by the application.
        The policy list is assigned as a property of this class and includes S3
        read access to ingest raw data as standard, but additional policies may
        be added as the need arises.

        Returns:
            list
        """
        responses = []

        for policy in self.aws_role_policies:
            try:
                response = self.client.attach_role_policy(
                    RoleName=self.dwh_db_role,
                    PolicyArn=policy['arn'],
                )
            except self.client.exceptions.EntityAlreadyExistsException:
                logger.info(
                    f"""'{policy["name"]}' already on '{self.dwh_db_role}'!"""
                )

            responses.append(response)
            logger.info(
                f"""'{policy["name"]}' added to '{self.dwh_db_role}'"""
            )

        return responses

    def detach_role_policies(self):
        """
        Detach the policies from the data warehouse role associated with this
        application. This function is invoked from the teardown() method of
        this class when the application is in `dry_run` mode.

        Returns:
            list
        """
        responses = []

        for policy in self.aws_role_policies:
            try:
                response = self.client.detach_role_policy(
                    RoleName=self.dwh_db_role,
                    PolicyArn=policy['arn'],
                )
            except self.client.exceptions.NoSuchEntityException:
                logger.info(f"'{policy['name']}' already detached!")

            logger.info(f"'{policy['name']}' detached")
            responses.append(response)

        return responses

    def delete_dwh_role(self):
        """
        Deletes the data warehouse role created by this application. This
        function is invoked from the teardown() method of this class which
        tears down the AWS infrastructure when the ETL operation completes
        in `dry_run` mode.

        Returns:
            json
        """
        try:
            response = self.client.delete_role(RoleName=self.dwh_db_role)
        except self.client.exceptions.NoSuchEntityException:
            logger.info(
                f"'{self.dwh_db_role}' already deleted!"
            )

        logger.info(f"'{self.dwh_db_role}' deleted")

        return response

    def teardown(self):
        """
        Detaches the policies from the application's data warehouse role then
        deletes it. This function is executed when the application is in
        `dry_run` mode; the AWS infrastructure is torn down upon completion.

        Returns:
            None
        """
        self.detach_role_policies()
        self.delete_dwh_role()

        logger.info('Teardown completed')
