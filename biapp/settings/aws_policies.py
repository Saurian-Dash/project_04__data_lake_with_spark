S3_FULL_ACCESS = {
  "name": "AmazonS3FullAccess",
  "arn": "arn:aws:iam::aws:policy/AmazonS3FullAccess",
}

EMR_SERVICE = {
  "name": "AmazonElasticMapReduceRole",
  "arn": "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole",
}

EMR_TRUST_RELATIONSHIP = {
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"Service": "elasticmapreduce.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }
  ]
}
