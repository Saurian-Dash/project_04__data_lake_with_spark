import boto3
import json

from biapp.core.logger import log
from biapp.settings.aws_policies import (
    EMR_FULL_ACCESS,
    EMR_TRUST_RELATIONSHIP,
)
from biapp.settings.config import (
    AWS_REGION,
    AWS_ROLE,
)

logger = log.setup_custom_logger(__name__)


class IAMOperator:

    def __init__(self):

        self.aws_role_policies = [EMR_FULL_ACCESS]
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
        Creates an IAM client with the AWS credentials configured in the AWS
        CLI.

        Returns:
            boto3.client
        """
        client = boto3.client(service_name='iam', region_name=AWS_REGION)
        logger.info('Client created')

        return client

    def create_role(self):
        """
        Creates an AWS role which allows the EMR cluster to interact with other
        AWS services. The role created by the application allows the cluster to
        read and write data to S3.
        """
        dwh_roles = self.client.list_roles()
        role_list = {x['RoleName'] for x in dwh_roles['Roles']}

        if self.dwh_db_role in role_list:
            self.detach_role_policies()
            self.delete_dwh_role()

        try:
            self.client.create_role(
                Path='/',
                RoleName=self.dwh_db_role,
                AssumeRolePolicyDocument=json.dumps(self.dwh_trust_policy),
                Description='Allows EMR to call AWS services on your behalf',
            )
            logger.info(f"'{self.dwh_db_role}' created")

        except self.client.exceptions.EntityAlreadyExistsException:
            logger.info(
                f"'{self.dwh_db_role}' already exists!"
            )

    def attach_role_policies(self):
        """
        Attaches a list of policies to the AWS role created by the application.
        """
        for policy in self.aws_role_policies:
            try:
                self.client.attach_role_policy(
                    RoleName=self.dwh_db_role,
                    PolicyArn=policy['arn'],
                )
                logger.info(
                    f"""'{policy["name"]}' added to '{self.dwh_db_role}'"""
                )

            except self.client.exceptions.EntityAlreadyExistsException:
                logger.info(
                    f"""'{policy["name"]}' already on '{self.dwh_db_role}'!"""
                )

    def detach_role_policies(self):
        """
        Detach the policies from the AWS role created by the application.
        """
        for policy in self.aws_role_policies:
            try:
                self.client.detach_role_policy(
                    RoleName=self.dwh_db_role,
                    PolicyArn=policy['arn'],
                )
                logger.info(f"'{policy['name']}' detached")

            except self.client.exceptions.NoSuchEntityException:
                logger.info(f"'{policy['name']}' already detached!")

    def delete_dwh_role(self):
        """
        Deletes the AWS role created by this application.
        """
        try:
            self.client.delete_role(RoleName=self.dwh_db_role)
            logger.info(f"'{self.dwh_db_role}' deleted")

        except self.client.exceptions.NoSuchEntityException:
            logger.info(
                f"'{self.dwh_db_role}' already deleted!"
            )
