# from socketserver import ThreadingUnixStreamServer
from aws_cdk import (
    Aspects,
    Duration,
    Stack,
    CfnOutput,
    CfnParameter,
    RemovalPolicy,
    aws_kms as kms,
    aws_sns as sns,
    aws_s3 as s3,
    aws_iam as iam,
    aws_stepfunctions as stepfunctions,
    aws_sns_subscriptions as subs,
    aws_lambda as lambda_
)
from constructs import Construct
from pathlib import Path
import random
import string
from cdk_nag import ( AwsSolutionsChecks, NagSuppressions )

class PackagedVodDownloaderStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        randomStr = generateRandomString(10)

        # Define template parameters
        email = CfnParameter(self, "email", type="String",
                                description="Email address to which SNS messages will be sent.").value_as_string
        mediaPackageRole = CfnParameter(self, "mediapackageRole",
                                type="String",
                                description="Role used by MediaPackage to access s3 and create MediaPackage VOD assets.",
                                default="MediaPackage_Default_Role").value_as_string
        mediaPackageCdnAuthSecretArn = CfnParameter(self, "mediaPackageCdnAuthSecretArn",
                                type="String",
                                description="AWS Secrets Manager secret used by MediaPackage Packaging Configuration for CDN Auth.",
                                default="").value_as_string

        # Create Master KMS Key for encrypting SNS
        masterKmsKey = kms.Key( self, "MasterKmsKey",
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.DESTROY
        )

        destinationBucket = self.createS3Bucket( "DestinationBucket" )

        # Create SNS Topic for notifications
        notificationTopic = sns.Topic(self, "SnsNotificationTopic",
            display_name="%s subscription topic" % (construct_id),
            master_key=masterKmsKey
        )
        notificationTopic.add_subscription(subs.EmailSubscription(email))

        # Create SNS Topic Dead Letter Topic
        deadLetterTopic = sns.Topic(self, "SnsDlqTopic",
            display_name="%s lambda dead letter queue" % (construct_id),
            master_key=masterKmsKey
        )
        deadLetterTopic.add_subscription(subs.EmailSubscription(email))

        # Create Lambda Layer
        vodDownloadLambdaFunctionName = "%s-VodDownloadFunction-%s" % (construct_id, randomStr)
        vodDownloadLambdaRole = self.createVodDownloadLambdaRole(vodDownloadLambdaFunctionName, destinationBucket, masterKmsKey )
        vodDownloadLayer = lambda_.LayerVersion( self, "VodDownloadLayer",
                                                description="Lambda layer containing VOD Download modules",
                                                code=lambda_.Code.from_asset("packaged_vod_downloader/layer"),
                                                compatible_architectures=[lambda_.Architecture.X86_64, lambda_.Architecture.ARM_64],
                                                compatible_runtimes=[lambda_.Runtime.PYTHON_3_9]
                                            )

        # Create Lambda Function to download VODs
        vodDownloadLambda = lambda_.Function( self,
                                            "vodDownloadFunction",
                                            function_name=vodDownloadLambdaFunctionName,
                                            description="Lambda function to download VOD Assets.",
                                            code=lambda_.Code.from_asset("packaged_vod_downloader/lambda"),
                                            runtime=lambda_.Runtime.PYTHON_3_9,
                                            handler="DownloadVod.fetchStream",
                                            role=vodDownloadLambdaRole,
                                            layers=[ vodDownloadLayer ],
                                            timeout=Duration.minutes(12),   # Slightly less than the maximum 15 min to allow lambda to stop gracefully
                                            memory_size=384,
                                            dead_letter_topic=deadLetterTopic
                                        )

        # Create role for Step Function Execution
        stateMachineName = "PackagedVodDownloaderStateMachine"
        ( stepFunctionRole, stepFunctionPolicy) = self.createStepFunctionRole( stateMachineName, notificationTopic, vodDownloadLambda, destinationBucket, mediaPackageRole, masterKmsKey, mediaPackageCdnAuthSecretArn )

        # Read State Machine definition file into string
        workflowDefinition = Path('packaged_vod_downloader/statemachine/vodDownloaderWorkflow.json').read_text()
        # Create State Machine
        cfn_state_machine = stepfunctions.CfnStateMachine(self, "vodDownloadStateMachine",
            role_arn=stepFunctionRole.role_arn,

            definition_string=workflowDefinition,
            definition_substitutions={
                "SNS_TOPIC": notificationTopic.topic_arn,
                "VOD_DOWNLOAD_LAMBDA": vodDownloadLambda.function_name
            },
            state_machine_name=stateMachineName,
            state_machine_type="STANDARD",
            tracing_configuration=stepfunctions.CfnStateMachine.TracingConfigurationProperty(
                enabled=True
            ),
            ## TODO: Commented out logging configuration as this is causing the following error:
            #        "Service: AWSStepFunctions; Status Code: 400; Error Code: InvalidLoggingConfiguration"
            # logging_configuration=stepfunctions.CfnStateMachine.LoggingConfigurationProperty(
            #     destinations=[
            #         stepfunctions.CfnStateMachine.LogDestinationProperty(
            #             cloud_watch_logs_log_group=stepfunctions.CfnStateMachine.CloudWatchLogsLogGroupProperty(
            #                 log_group_arn="arn:aws:logs:%s:%s:log-group:/aws/vendedlogs/states/%s:*" % (self.region, self.account, stateMachineName)
            #             )
            #         )
            #     ],
            #     include_execution_data=False,
            #     level="ALL"
            # )
        )
        cfn_state_machine.add_depends_on(stepFunctionRole.node.default_child)
        cfn_state_machine.add_depends_on(stepFunctionPolicy.node.default_child)

        CfnOutput(self, "StateMachineArn", value=cfn_state_machine.attr_arn)
        CfnOutput(self, "DestinationBucketName", value=destinationBucket.bucket_name)


        Aspects.of(self).add(AwsSolutionsChecks())
        NagSuppressions.add_resource_suppressions( destinationBucket, [
            {
                "id": "AwsSolutions-S1",
                "reason": "Bucket is restricted to only allow prinipals access. Logging is not required."
            }
        ])
        NagSuppressions.add_resource_suppressions (vodDownloadLambdaRole, [
            {
                "id": "AwsSolutions-IAM5",
                "reason": "Wildcard required in IAM role to write logs to CloudWatch, write to the destination path in S3 Bucket and access all MediaPackage-VOD Assets."
            }
        ], apply_to_children=True)
        NagSuppressions.add_resource_suppressions (stepFunctionPolicy, [
            {
                "id": "AwsSolutions-IAM5",
                "reason": "State Machine needs to be able to access the MediaPackage Packaging Groups and Assets specified in the input. This requires a wildcard."
            }
        ])
        NagSuppressions.add_resource_suppressions (cfn_state_machine, [
            {
                "id": "AwsSolutions-SF1",
                "reason": "########## TODO ##########: Unable to configure logging on state machine using CDK. Further investigation required as this is most likely a defect."
            }
        ])


    def createS3Bucket(self, resourceName):
        # Create S3 Bucket to store downloaded MediaPackage VOD Assets
        destinationBucket = s3.Bucket(self, resourceName,
            # cdk-nag says it should be versioned but it makes no use for a media use case
            # versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.RETAIN
        )

        # Define bucket policy to deny any requests not over SSL - Required by cdk_nag
        bucketPolicyStatement = iam.PolicyStatement( # Restrict to listing and describing tables
            principals=[iam.AnyPrincipal()],
            actions=["s3:*"],
            effect= iam.Effect.DENY,
            resources=[
                    destinationBucket.bucket_arn,
                    "%s/*" % destinationBucket.bucket_arn
                ],
            conditions={ "Bool": {"aws:SecureTransport": "false"} }
        )

        # Add policy to bucket - Required by cdk_nag
        destinationBucket.add_to_resource_policy( bucketPolicyStatement )

        return destinationBucket


    def createVodDownloadLambdaRole(self, vodDownloadLambdaFunctionName, destinationBucket, masterKmsKey):
        vodDownloadLambdaRole = iam.Role(self, "VodDownloadLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )

        # Allow access to CloudWatch for logging
        vodDownloadLambdaRole.add_to_policy(iam.PolicyStatement(
            actions=[
                "logs:CreateLogStream",
                "logs:CreateLogGroup",
                "logs:PutLogEvents"
            ],
            resources=[
                "arn:aws:logs:%s:%s:log-group:/aws/lambda/%s" % (self.region, self.account, vodDownloadLambdaFunctionName),
                "arn:aws:logs:%s:%s:log-group:/aws/lambda/%s:*" % (self.region, self.account, vodDownloadLambdaFunctionName)
            ]
        ))
        # Download Lambda needs to be able to read S3.
        vodDownloadLambdaRole.add_to_policy(iam.PolicyStatement(
            actions=[ "s3:ListBucket" ],
            resources=[ destinationBucket.bucket_arn ]
        ))
        # Download Lambda needs to be able to write to S3.
        vodDownloadLambdaRole.add_to_policy(iam.PolicyStatement(
            actions=[
                "s3:PutObject",
                "s3:GetObject"
                ],
            resources=[ "%s/*" % destinationBucket.bucket_arn ]
        ))
        # Download Lambda needs to be able to create a KMS Data Key
        vodDownloadLambdaRole.add_to_policy(iam.PolicyStatement(
            actions=["kms:GenerateDataKey"],
            resources=[
                "arn:aws:kms:%s:%s:key/%s" % (self.region, self.account, masterKmsKey.key_id)
            ]
        ))

        return vodDownloadLambdaRole
    

    def createStepFunctionRole(self, stateMachineName, notificationTopic, vodDownloadLambda, destinationBucket, mediaPackageRole, masterKmsKey, mediaPackageCdnAuthSecretArn):
        stepFunctionRole = iam.Role(self, "StateMachineRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com")
        )

        stepFunctionPolicy = iam.Policy(self, "StateMachinePolicy",
            statements=[
                # Allow read access to all S3 buckets
                iam.PolicyStatement(
                    actions=["s3:ListBucket"],
                    resources=[ destinationBucket.bucket_arn ]
                ),
                # Allow access to MediaPackage VOD
                iam.PolicyStatement(
                    actions=[
                        "mediapackage-vod:CreateAsset",
                        "mediapackage-vod:DeleteAsset",
                        "mediapackage-vod:DescribeAsset"
                        ],
                    resources=[ "arn:aws:mediapackage-vod:%s:%s:assets/*" % (self.region, self.account) ]
                ),
                # Allow access to MediaPackage VOD Packaging Groups
                iam.PolicyStatement(
                    actions=[
                        "mediapackage-vod:DescribePackagingGroup"
                        ],
                    resources=[ "arn:aws:mediapackage-vod:%s:%s:packaging-groups/*" % (self.region, self.account) ]
                ),
                # Allow State Machine to assume role passed into execution
                # Generally, it is expected this would be the MediaPackage_Default_Role
                iam.PolicyStatement(
                    actions=["iam:PassRole"],
                    resources=[
                        "arn:aws:iam::%s:role/%s" % (self.account, mediaPackageRole)
                    ]
                ),
                # Allow State Machine to publish to SNS Topic
                iam.PolicyStatement(
                    actions=["sns:Publish"],
                    resources=[
                        notificationTopic.topic_arn
                    ]
                ),
                # Allow State Machine to invoke Lambda Function
                # Required to execute VOD download function
                iam.PolicyStatement(
                    actions=["lambda:InvokeFunction"],
                    resources=[
                        vodDownloadLambda.function_arn
                    ]
                ),
                # Allow state machine to create a KMS Data Key
                iam.PolicyStatement(
                    actions=[
                        "kms:GenerateDataKey",
                        "kms:Decrypt"
                    ],
                    resources=[
                        "arn:aws:kms:%s:%s:key/%s" % (self.region, self.account, masterKmsKey.key_id)
                    ]
                )
            ]
        )

        stepFunctionRole.attach_inline_policy(stepFunctionPolicy)

        # Allow state machine to access secret from secrets manager
        # This is to allow the role to retrieve the secret used to secure the packaging configuration endpoints
        # with CDN Auth
        # TODO: Need to implement conditional logic to optionalling include support for CdnAuth
        # This implementaion is not currently working.
        # if not cdnAuthDisabled:
        #     stepFunctionRole.add_to_policy(iam.PolicyStatement(
        #         actions=[
        #             "secretsmanager:GetSecretValue"
        #         ],
        #         resources=[
        #             mediaPackageCdnAuthSecretArn
        #         ]
        #     ))

        return ( stepFunctionRole, stepFunctionPolicy )


def generateRandomString(length):
    character_set = string.ascii_letters
    return ''.join(random.choice(character_set) for i in range(length))
