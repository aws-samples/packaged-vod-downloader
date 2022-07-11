# General Overview

The Packaged VOD Downloader Workflow simplifies the process of repackaging HLS/TS Video On Demand assets to one or more output packaging formats (including HLS, DASH and CMAF).

The Packaged VOD Downloader can be particularly useful when users want to leverage the advanced packaging capabilities of AWS Elemental MediaPackage but do not require a packaging on the fly solution.

![Workflow Architecture Diagram](/images/ArchitectureDiagram.png)

** This project is intended for education purposes only and not for production usage. **

This sample leverages [AWS Elemental MediaPackage](https://aws.amazon.com/mediapackage/), [AWS Lambda](https://aws.amazon.com/lambda/), [Amazon S3](https://aws.amazon.com/s3) and [AWS Step Functions](https://aws.amazon.com/step-functions) to execute the workflow.

At a high level the workflow uses a Step Function to orchestrate the following steps:
1) Create an AWS Elemental MediaPackage VOD Asset (if it does not already exist)
2) For each Packaging Configuration in the Packaging Group a VOD asset is downloaded to S3 using a Lambda function
3) Send notification with details of downloaded asset

Important notes:
1. Workflow does not delete the AWS Elemental MediaPackage VOD asset after it has been successfully harvested.
2. Workflow will skip downloading a specific endpoint if objects already exist with that key in the S3 destination.
3. Workflow does NOT handle downloading assets if updates have occurred in AWS Elemental MediaPackage VOD. To re-download endpoint which has previously been downloaded to the same target location the existing objects for that endpoint must be removed.
4. A CloudFront Distribution is recommended to scale the delivery of content to users. When implementing the CloudFront Distribution consideration should be given to how the content can be appropriately secured from unauthorized access. Using AWS Elemental MediaPackage in conjunction with a DRM (Digital Rights Management) Provider is a common way to secure content.
5. Consideration should be given to the specific configuration of the S3 bucket and whether additional access logging may be required for highly sensitive content and what an appropriate data retention period may be for the workflow output.



Below is an example of a succesful state machine execution as it appears in the AWS Console.

![Step Function State Machine Screenshot](/images/workflow.png)

# Getting Started

⚠️ **IMPORTANT NOTE:** **Deploying this demo application in your AWS account will create and consume AWS resources, which will cost money.**

To get the demo running in your own AWS account, follow these instructions.

1. If you do not have an AWS account, please see [How do I create and activate a new Amazon Web Services account?](https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/)
2. Log into the [AWS console](https://console.aws.amazon.com/) if you are not already. Note: If you are logged in as an IAM user, ensure your account has permissions to create and manage the necessary resources and components for this application.

## Deployment

This reference template deploys the Packaged VOD Downloader to the default AWS account.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the `.venv`
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies for CDK and the lambda layer.

```
$ pip install -r requirements.txt    # requirements for CDK
$ pip install -r packaged_vod_downloader/layer/requirements.txt  -t packaged_vod_downloader/layer/python  # requirements for Lambda Layer
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
```

Once the synthesize command is successful, you can deploy the workflow. Ensure you configure the email parameter with a valid recipient address. SNS alerts will be sent to this address.

```
$ cdk deploy --parameters email=user@sample.com --parameters mediapackageRole=MediaPackage_Default_Role --parameters mediaPackageCdnAuthSecretArn=arn:aws:secretsmanager:ap-southeast-2:0123456789012:secret:MyMediaPackageCdnAuthSecret-XXXXXX
```

The 'email' parameter (required) specifies the distination for SNS topic notifications. During the installation emails will be sent to the specified email address to subscribe to two SNS topics. Click in the link to accept the email.

The 'medipackageRole' parameter (optional) defines the role to be used for the creation of AWS Elemental MediaPackage VOD assets. This role needs to have permission to create the AWS Elemental MediaPackage VOD assets and access the source content to be ingested. Then default value MediaPackage_Default_Role.

The 'mediaPackageCdnAuthSecret' parameter (optional) defines the AWS Secrets Manager Secret containing the AWS Elemental MediaPackage [CDN Auth header secret](https://docs.aws.amazon.com/mediapackage/latest/ug/cdn-auth.html) for the Packaging Configuration being used with the sample. This parameter is provided to simplify setting up the AWS Sample with a packaging configuration using CDN Auth. Setting this parameter will configure the IAM role used by the AWS Step Function with permissions to access the specified secret. This allows the Step Function to access the secret and pass value to AWS Lambda to be included in requests to download resources. If more than one packaging configuration with CDN Auth enabled will be used with the sample the IAM policy associated with the Step Function role will need to be manually modified.

** Note: Enabling CDN Auth on AWS Elemental MediaPackage Packaging Groups is highly recommended to restrict access to the endpoints. **

If the solution is being deployed into the default profile no additional environment variables need to be set. If the solution is being deployed using the non-default profile the AWS CLI environment variables should be used to specify the access key id, secret access key and region. Below is an example of the environment variables which need to be set.
```
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_DEFAULT_REGION=us-west-2
```

### Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

# Usage

Once deployed a Step Function Execution can be started for each set of AWS Elemental MediaPackage VOD outputs required.

## Step Function Execution Format

Below is an example of an execution:
```
{
  "createAssetRequest": {
    "Id": "MyMediaPackageVodAssetId1",                                            # User MediaPackage VOD Asset identifier
    "PackagingGroupId": "MyMediaPackagePackagingGroupId",                         # MediaPacakge Packaging Group ID to use for asset creation
    "DestinationBucket": "my-test-bucket",                                        # S3 bucket where objects will be downloaded
    "DestinationPath": "vod-downloads"                                            # path in S3 where objects will be downloaded
    "SourceArn": "arn:aws:s3:::sample-bucket/sample-key/index.m3u8",              # Input may be an m3u8 or smil file
    "SourceRoleArn": "arn:aws:iam::999999999999:role/MediaPackage_Default_Role",  # This is the role to be used for the 
                                                                                  # MediaPackage VOD Asset
  }
}
```

Prior to starting an execution:
1. The MediaPackaging Packaging Group needs to be created (including associated Packaging Configurations)
1. HLS/TS (or smil) source content needs to be uploaded to an S3 bucket
1. The SourceRoleArn will require permission to access (and write to) the source location

The name of the Step Function State Machine deployed by the template is listed as an output of the deployed CloudFormation Stack.
This can be retrieved under the 'Outputs' tab in the Stack in the CloudFormation Stack.
It can also be retrieve by executing the following CLI command:

    aws cloudformation describe-stacks --stack-name <STACK-NAME> | grep Output

## Command Line Execution Submission

```
aws stepfunctions start-execution --state-machine-arn arn:aws:states:ap-southeast-2:XXXXXXXXXXXX:stateMachine:StateMachine-jvkp8UEK01Ve \
  --input "{
            \"createAssetRequest\": {
              \"Id\": \"MyMediaPackageVodAssetId1\",                                             # User MediaPackage VOD Asset identifier
              \"PackagingGroupId\": \"MyMediaPackagePackagingGroupId\",                          # MediaPacakge Packaging Group ID to use for asset creation
              \"SourceArn\": \"arn:aws:s3:::sample-bucket/sample-key/index.m3u8\",               # Source content
              \"SourceRoleArn\": \"arn:aws:iam::XXXXXXXXXXXX:role/MediaPackage_Default_Role\",   # Role used to create MediaPackage VOD asset
              \"DestinationBucket\": \"my-test-bucket\",                                         # S3 bucket where objects will be downloaded
              \"DestinationPath\": \"vod-downloads\"                                             # path in S3 where objects will be downloaded
            }
          }"
```


## Console Execution Submission

Executions can also be started via the AWS Console.

To start an execution via the console:
1. Open AWS Console
1. Navigate to the Step Functions service
1. If not already selected, select 'State Machines' from the left hand menu
1. Select deployed State Machine (most likely called 'PackagedVodDownloaderStateMachine')
1. Select 'Start Execution' in the top right hand corner of the console
1. Enter execution input into the input field and click 'Start Execution'

# Known Limitations
1. DASH Download Fails if stream contains JPG thumbnails - Enhancements required to download script

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

