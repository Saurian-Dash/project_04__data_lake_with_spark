import boto3

import biapp.core.logger.log as log
from biapp.settings.config import (
    AWS_REGION,
    AWS_ROLE,
    EMR_CONFIG,
    S3_CODE_PATH,
    S3_INPUT_DATA,
    S3_OUTPUT_DATA,
)

logger = log.setup_custom_logger(__name__)


class EMROperator:

    def __init__(self):

        self.client = self.create_emr_client()

    def create_emr_client(self):

        client = boto3.client('emr', region_name=AWS_REGION)
        logger.info('Client created')

        return client

    def create_emr_cluster(self, config=EMR_CONFIG):

        response = self.client.run_job_flow(
            Name='spark-emr-cluster',
            ReleaseLabel='emr-5.28.0',
            LogUri=config['EMR_LOG_URI'],
            Applications=[
                {
                    'Name': 'Spark'
                },
            ],
            Configurations=[
                {
                    "Classification": "spark-env",
                    "Configurations": [
                        {
                            "Classification": "export",
                            "Properties": {
                                "PYSPARK_PYTHON": "/usr/bin/python3"
                            }
                        }
                    ]
                }
            ],
            Instances={
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': config['EMR_MARKET'],
                        'InstanceRole': 'MASTER',
                        'InstanceType': config['EMR_INSTANCE_TYPE'],
                        'InstanceCount': int(config['EMR_MASTER_NODES']),
                    },
                    {
                        'Name': "Slave nodes",
                        'Market': config['EMR_MARKET'],
                        'InstanceRole': 'CORE',
                        'InstanceType': config['EMR_INSTANCE_TYPE'],
                        'InstanceCount': int(config['EMR_SLAVE_NODES']),
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': False,
                'TerminationProtected': False,
            },
            Steps=[
                {
                    'Name': 'Setup Debugging',
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['state-pusher-script']
                    }
                },
                {
                    'Name': 'Setup - copy files',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            'aws',
                            's3',
                            'cp',
                            f's3://{S3_CODE_PATH}',
                            '/home/hadoop/',
                            '--recursive'
                        ]
                    }
                },
                {
                    'Name': 'Run Spark',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            'spark-submit',
                            '/home/hadoop/app.py',
                            S3_INPUT_DATA,
                            S3_OUTPUT_DATA
                        ]
                    }
                }
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole=AWS_ROLE
        )

        logger.info(f"Cluster ID: {response['JobFlowId']} created")

        return response
