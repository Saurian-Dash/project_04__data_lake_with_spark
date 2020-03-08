import configparser as configparser
import os

config = configparser.ConfigParser()

# read envs.cfg file in this directory
this_dir = os.path.dirname(os.path.abspath(__file__))
cfg_file = os.path.join(this_dir, 'envs.cfg')
config.read(cfg_file)

# aws settings
AWS_REGION = config.get('AWS', 'AWS_REGION')
AWS_ROLE = config.get('AWS', 'AWS_ROLE')

# emr settings
EMR_INSTANCE_TYPE = config.get('EMR', 'EMR_INSTANCE_TYPE')
EMR_LOG_URI = config.get('EMR', 'EMR_LOG_URI')
EMR_MARKET = config.get('EMR', 'EMR_MARKET')
EMR_MASTER_NODES = config.get('EMR', 'EMR_MASTER_NODES')
EMR_SLAVE_NODES = config.get('EMR', 'EMR_SLAVE_NODES')

# emr cluster configuration
EMR_CONFIG = {
    'EMR_INSTANCE_TYPE': EMR_INSTANCE_TYPE,
    'EMR_LOG_URI': EMR_LOG_URI,
    'EMR_MARKET': EMR_MARKET,
    'EMR_MASTER_NODES': EMR_MASTER_NODES,
    'EMR_SLAVE_NODES': EMR_SLAVE_NODES,
}

# s3 data storage
S3_INPUT_DATA = config.get('S3_DATA', 'S3_INPUT_DATA')
S3_OUTPUT_DATA = config.get('S3_DATA', 'S3_OUTPUT_DATA')

# s3 infrastructure
S3_CODE_BUCKET = config.get('S3_INFRA', 'S3_CODE_BUCKET')
S3_DATA_LAKE = config.get('S3_INFRA', 'S3_DATA_LAKE')
