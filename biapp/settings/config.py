import configparser as configparser


# read config file
config = configparser.ConfigParser()
config.read('biapp/settings/envs.cfg')

# aws credentials
AWS_REGION = config.get('AWS', 'AWS_REGION')
AWS_ROLE = config.get('AWS', 'AWS_ROLE')

# emr credentials
EMR_INSTANCE_TYPE = config.get('EMR', 'EMR_INSTANCE_TYPE')
EMR_LOG_URI = config.get('EMR', 'EMR_LOG_URI')
EMR_MARKET = config.get('EMR', 'EMR_MARKET')
EMR_MASTER_NODES = config.get('EMR', 'EMR_MASTER_NODES')
EMR_SLAVE_NODES = config.get('EMR', 'EMR_SLAVE_NODES')

# emr config
EMR_CONFIG = {
    'EMR_INSTANCE_TYPE': EMR_INSTANCE_TYPE,
    'EMR_LOG_URI': EMR_LOG_URI,
    'EMR_MARKET': EMR_MARKET,
    'EMR_MASTER_NODES': EMR_MASTER_NODES,
    'EMR_SLAVE_NODES': EMR_SLAVE_NODES,
}

# s3 data lake
S3_INPUT_DATA = config.get('S3_DATA_LAKE', 'S3_INPUT_DATA')
S3_OUTPUT_DATA = config.get('S3_DATA_LAKE', 'S3_OUTPUT_DATA')

# s3 infrastructure
S3_CODE_PATH = config.get('S3_INFRA', 'S3_CODE_PATH')
