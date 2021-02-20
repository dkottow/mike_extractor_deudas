import logging
from typing import Any, Dict, List
import json
import time

import boto3

from config import Config as config
from utils import parse_mike_key
from textract_response_parser import TextractResponseParser

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ProcessType:
    DETECTION = 1
    ANALYSIS = 2


class TextractClientAdapter:
    # Clientes
    textract = boto3.client("textract")
    sqs = boto3.client("sqs")
    sns = boto3.client("sns")

    # Variables de entorno.
    roleArn = config.ROLE_ARN
    bucket = config.S3_BUCKET
    seconds_to_await_for_response = config.SECONDS_TO_AWAIT_FOR_TEXTEXTRACT_RESPONSE

    def __init__(self, key: str, processType: int):

        self.processType = processType
        self.key = key
        self.parsed_key = parse_mike_key(key)

        self.jobId = ""
        self.sqsQueueUrl = ""
        self.snsTopicArn = ""

        self.request_responses: List = []
        self.results: Dict = {}

    def ProcessDocument(self):
        jobFound = False
        validType = False

        # Enviar a procesar el archivo según el tipo de detección solicitado.
        if self.processType == ProcessType.DETECTION:
            response = self.textract.start_document_text_detection(
                DocumentLocation={"S3Object": {"Bucket": self.bucket, "Name": self.key}},
                NotificationChannel={"RoleArn": self.roleArn, "SNSTopicArn": self.snsTopicArn},
            )
            # print('Processing type: Detection')
            validType = True

        if self.processType == ProcessType.ANALYSIS:
            response = self.textract.start_document_analysis(
                DocumentLocation={"S3Object": {"Bucket": self.bucket, "Name": self.key}},
                FeatureTypes=["TABLES"],
                NotificationChannel={"RoleArn": self.roleArn, "SNSTopicArn": self.snsTopicArn},
            )
            # print('Processing type: Analysis')
            validType = True

        if validType is False:
            msg = f"{self.key} - Invalid processing type. Choose Detection or Analysis."
            logging.exception(msg)
            raise Exception(msg)

        # Notificar que la detección empezó
        logger.info(f"[{self.key}] - TextExtract Started. Job Id: {response['JobId']}")

        # Buscar los resultados
        while jobFound is False:
            # Buscar el trabajo en la cola
            sqsResponse = self.sqs.receive_message(
                QueueUrl=self.sqsQueueUrl, MessageAttributeNames=["ALL"], MaxNumberOfMessages=10
            )

            # Si encontré el id del trabajo
            if sqsResponse:

                # Verifico si hay mensajes en la cola.
                if "Messages" not in sqsResponse:
                    logger.info(f"[{self.key}] - Awaiting {self.seconds_to_await_for_response} seconds...")

                    # Si textextact aún no termina, espero y continuo en la siguiente iteración.
                    time.sleep(self.seconds_to_await_for_response)
                    continue

                for message in sqsResponse["Messages"]:
                    notification = json.loads(message["Body"])
                    textMessage = json.loads(notification["Message"])

                    if str(textMessage["JobId"]) == response["JobId"]:
                        logger.info(f"[{self.key}] - TextExtract Matching Job Found: {textMessage['JobId']}")
                        jobFound = True

                        # Obtengo los resultados y elimino el mensaje de la cola.
                        self.GetResultsFromTextExtract(textMessage["JobId"])
                        self.sqs.delete_message(QueueUrl=self.sqsQueueUrl, ReceiptHandle=message["ReceiptHandle"])
                    else:
                        logger.error("Job didn't match:" + str(textMessage["JobId"]) + " : " + str(response["JobId"]))
                    # Delete the unknown message. Consider sending to dead letter queue
                    self.sqs.delete_message(QueueUrl=self.sqsQueueUrl, ReceiptHandle=message["ReceiptHandle"])

    def CreateTopicandQueue(self):
        # TODO: Refactorizar esto a una sola cola!!

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

    def GetResultsFromTextExtract(self, jobId):
        maxResults = 1000
        paginationToken = None
        finished = False

        while finished is False:

            response = None

            # Obtener los resultados desde textextract según el tipo de analisis realizado:
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

            # Agregar los resultados a responses
            self.request_responses.append(response)
            logging.info(
                f"[{self.key}] - TextExtract: Detected Document Text - Pages: {response['DocumentMetadata']['Pages']}"
            )

            if "NextToken" in response:
                paginationToken = response["NextToken"]
            else:
                finished = True

    def GetResults(self, parsed=True) -> Dict[str, Any]:

        self.results = self.request_responses[0]
        if len(self.request_responses) > 0:
            for request_response in self.request_responses[1:]:
                self.results["Blocks"] += request_response["Blocks"]

        if parsed is False:
            return self.results

        parsed_response = TextractResponseParser(self.results)

        return {
            "text": parsed_response.get_text(),
            "tables": parsed_response.get_tables(),
            "forms": parsed_response.get_forms(),
        }