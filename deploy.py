from biapp.core.operators.emr import EMROperator
from biapp.core.operators.iam import IAMOperator
from biapp.core.operators.s3 import S3Operator
from biapp.settings.config import S3_CODE_PATH


def main():

    # instantiate aws clients
    iam = IAMOperator()
    emr = EMROperator()
    s3 = S3Operator()

    # setup aws policies
    iam.create_role()
    iam.attach_role_policies()

    # deploy aws infrastructure
    s3.deploy_code(bucket=S3_CODE_PATH)
    emr.create_emr_cluster()


if __name__ == '__main__':
    main()
