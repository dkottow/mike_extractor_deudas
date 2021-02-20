from typing import Any
import boto3


class DocumentLoader():
    def __init__(self) -> None:
        self.s3 = boto3.resource('s3')

    def get_document(self, bucketname, itemname) -> Any:
        obj = self.s3.Object(bucketname, itemname)
        body = obj.get()['Body'].read()

        return body
