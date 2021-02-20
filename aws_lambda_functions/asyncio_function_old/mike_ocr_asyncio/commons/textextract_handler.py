import logging
from typing import List
import boto3
import json
import sys
import time
import asyncio

from ..config import Config as config


class ProcessType:
    DETECTION = 1
    ANALYSIS = 2


class TextExtractHandler:
    textract = boto3.client("textract")
    sqs = boto3.client("sqs")
    sns = boto3.client("sns")

    def __init__(self, role: str, bucket: str, document: str, processType: int, filename: str):

        self.roleArn = role
        self.bucket = bucket
        self.document = document
        self.processType = processType
        self.filename = filename
        self.seconds_to_await_for_response = config.SECONDS_TO_AWAIT_FOR_TEXTEXTRACT_RESPONSE

        self.jobId = ""
        self.sqsQueueUrl = ""
        self.snsTopicArn = ""

        self.responses: List = []

    async def ProcessDocument(self):
        jobFound = False
        validType = False

        # Determine which type of processing to perform
        if self.processType == ProcessType.DETECTION:
            response = self.textract.start_document_text_detection(
                DocumentLocation={"S3Object": {"Bucket": self.bucket, "Name": self.document}},
                NotificationChannel={"RoleArn": self.roleArn, "SNSTopicArn": self.snsTopicArn},
            )
            # print('Processing type: Detection')
            validType = True

        if self.processType == ProcessType.ANALYSIS:
            response = self.textract.start_document_analysis(
                DocumentLocation={"S3Object": {"Bucket": self.bucket, "Name": self.document}},
                FeatureTypes=["TABLES"],
                NotificationChannel={"RoleArn": self.roleArn, "SNSTopicArn": self.snsTopicArn},
            )
            # print('Processing type: Analysis')
            validType = True

        if validType is False:
            logging.exception("{} - Invalid processing type. Choose Detection or Analysis.".format(self.filename))
            return

        logging.info("[{}] - TextExtract Start Job Id: {}".format(self.filename, response["JobId"]))
        dotLine = 0

        while jobFound is False:
            sqsResponse = self.sqs.receive_message(
                QueueUrl=self.sqsQueueUrl, MessageAttributeNames=["ALL"], MaxNumberOfMessages=10
            )

            if sqsResponse:

                if "Messages" not in sqsResponse:
                    # if dotLine < 40:
                    #     print('.', end='')
                    #     dotLine = dotLine + 1
                    # else:
                    #     print()
                    #     dotLine = 0
                    # sys.stdout.flush()
                    logging.info(
                        "[{}] - Awaiting {} seconds...".format(self.filename, self.seconds_to_await_for_response)
                    )

                    await asyncio.sleep(self.seconds_to_await_for_response)

                    continue

                for message in sqsResponse["Messages"]:
                    notification = json.loads(message["Body"])
                    textMessage = json.loads(notification["Message"])
                    # print(textMessage['JobId'])
                    # print(textMessage['Status'])
                    if str(textMessage["JobId"]) == response["JobId"]:
                        logging.info(
                            "{} - TextExtract Matching Job Found: {}".format(self.filename, textMessage["JobId"])
                        )
                        jobFound = True
                        self.GetResults(textMessage["JobId"])
                        self.sqs.delete_message(QueueUrl=self.sqsQueueUrl, ReceiptHandle=message["ReceiptHandle"])
                    else:
                        print("Job didn't match:" + str(textMessage["JobId"]) + " : " + str(response["JobId"]))
                    # Delete the unknown message. Consider sending to dead letter queue
                    self.sqs.delete_message(QueueUrl=self.sqsQueueUrl, ReceiptHandle=message["ReceiptHandle"])

        # print('Done!')

    def CreateTopicandQueue(self):

        millis = str(int(round(time.time() * 1000)))

        # Create SNS topic
        snsTopicName = "AmazonTextractTopic" + millis

        topicResponse = self.sns.create_topic(Name=snsTopicName)
        self.snsTopicArn = topicResponse["TopicArn"]

        # create SQS queue
        sqsQueueName = "AmazonTextractQueue" + millis
        self.sqs.create_queue(QueueName=sqsQueueName)
        self.sqsQueueUrl = self.sqs.get_queue_url(QueueName=sqsQueueName)["QueueUrl"]

        attribs = self.sqs.get_queue_attributes(QueueUrl=self.sqsQueueUrl, AttributeNames=["QueueArn"])["Attributes"]

        sqsQueueArn = attribs["QueueArn"]

        # Subscribe SQS queue to SNS topic
        self.sns.subscribe(TopicArn=self.snsTopicArn, Protocol="sqs", Endpoint=sqsQueueArn)

        # Authorize SNS to write SQS queue
        policy = """{{
  "Version":"2012-10-17",
  "Statement":[
    {{
      "Sid":"MyPolicy",
      "Effect":"Allow",
      "Principal" : {{"AWS" : "*"}},
      "Action":"SQS:SendMessage",
      "Resource": "{}",
      "Condition":{{
        "ArnEquals":{{
          "aws:SourceArn": "{}"
        }}
      }}
    }}
  ]
}}""".format(
            sqsQueueArn, self.snsTopicArn
        )

        response = self.sqs.set_queue_attributes(QueueUrl=self.sqsQueueUrl, Attributes={"Policy": policy})

    def DeleteTopicandQueue(self):
        self.sqs.delete_queue(QueueUrl=self.sqsQueueUrl)
        self.sns.delete_topic(TopicArn=self.snsTopicArn)

    def GetResults(self, jobId):
        maxResults = 1000
        paginationToken = None
        finished = False

        while finished is False:

            response = None

            if self.processType == ProcessType.ANALYSIS:
                if paginationToken is None:
                    response = self.textract.get_document_analysis(JobId=jobId, MaxResults=maxResults)
                else:
                    response = self.textract.get_document_analysis(
                        JobId=jobId, MaxResults=maxResults, NextToken=paginationToken
                    )

            if self.processType == ProcessType.DETECTION:
                if paginationToken is None:
                    response = self.textract.get_document_text_detection(JobId=jobId, MaxResults=maxResults)
                else:
                    response = self.textract.get_document_text_detection(
                        JobId=jobId, MaxResults=maxResults, NextToken=paginationToken
                    )

            self.responses.append(response)
            logging.info(
                "[{}] - TextExtract: Detected Document Text - Pages: {}".format(
                    self.filename, response["DocumentMetadata"]["Pages"]
                )
            )

            if "NextToken" in response:
                paginationToken = response["NextToken"]
            else:
                finished = True
