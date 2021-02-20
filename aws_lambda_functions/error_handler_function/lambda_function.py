import io
import json
import logging
from zipfile import ZipFile
import pathlib
import time

import boto3

from utils import parse_mike_key, parsed_mike_key_to_str
from config import Config as config

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: dict, context):

    s3_client = boto3.client("s3")

    try:
        error_unparsed = event["responsePayload"]["errorMessage"]
        error_unparsed = error_unparsed.split("]")
        key = error_unparsed[0].replace("[", "").strip()
        parsed_key = parse_mike_key(key)

        error_message = error_unparsed[1].strip()

        payload = {"key": key, "error_message": error_message, "event": event}

        s3_client.upload_fileobj(
            io.BytesIO(json.dumps(payload).encode()),
            Bucket=config.S3_BUCKET,
            Key=parsed_mike_key_to_str(parsed_key, new_bandeja="Errores", new_extension="json"),
        )
        return payload

    except Exception as e:
        logger.exception(f"Unexpected error in error handler: {e}. Event: {event}")
