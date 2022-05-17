# Axiom CloudFront Lambda [![CI](https://github.com/axiomhq/axiom-cloudfront-lambda/actions/workflows/ci.yaml/badge.svg)](https://github.com/axiomhq/axiom-cloudfront-lambda/actions/workflows/ci.yaml)

Easy to use AWS CloudFormation template to deploy a Bucket and a Lambda to send
logs from your CloudFront to [Axiom](https://axiom.co).

## Guide

1. Create an account at [Axiom Cloud](https://cloud.axiom.co)
2. Create a dataset and an API token with ingest permission for that dataset
3. If S3 bucket exists, [![Launch Stack](https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png)](https://console.aws.amazon.com/cloudformation/home?#/stacks/new?stackName=CloudFront-Axiom&templateURL=https://axiom-cloudformation-stacks.s3.amazonaws.com/axiom-cloudfront-lambda-base-cloudformation-stack.yaml)
4. If S3 bucket doesn't exists, Launch the stack: [![Launch Stack](https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png)](https://console.aws.amazon.com/cloudformation/home?#/stacks/new?stackName=CloudFront-Axiom&templateURL=https://axiom-cloudformation-stacks.s3.amazonaws.com/axiom-cloudfront-lambda-cloudformation-stack.yaml)
5. Set up your CloudFront to store logs in the bucket you specified
