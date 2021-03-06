from core.operators.emr import EMROperator
from core.operators.iam import IAMOperator
from core.operators.s3 import S3Operator
from settings.config import S3_CODE_BUCKET, S3_DATA_LAKE, S3_LOGS


def main():

    # instantiate aws clients
    iam = IAMOperator()
    emr = EMROperator()
    s3 = S3Operator()

    # setup aws policies
    iam.create_role()
    iam.attach_role_policies()

    # deploy aws infrastructure
    s3.create_bucket(bucket=S3_CODE_BUCKET)
    s3.create_bucket(bucket=S3_DATA_LAKE)
    s3.create_bucket(bucket=S3_LOGS)
    s3.deploy_code(bucket=S3_CODE_BUCKET)
    emr.create_emr_cluster()


if __name__ == '__main__':
    main()
