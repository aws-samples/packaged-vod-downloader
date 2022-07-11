import aws_cdk as core
import aws_cdk.assertions as assertions

from packaged_vod_downloader.packaged_vod_downloader_stack import PackagedVodDownloaderStack

# example tests. To run these tests, uncomment this file along with the example
# resource in packaged_vod_downloader/packaged_vod_downloader_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = PackagedVodDownloaderStack(app, "packaged-vod-downloader")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
