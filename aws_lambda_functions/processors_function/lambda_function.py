import io
import json
import logging

import boto3
import pandas as pd

from config import Config as config
from utils import parse_mike_key, parsed_mike_key_to_str

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):

    s3_client = boto3.client("s3")

    key_to_json = event["Records"][0]["s3"]["object"]["key"]

    file_s3_obj = s3_client.get_object(Bucket=config.S3_BUCKET, Key=key_to_json)
    file_bytes = file_s3_obj["Body"].read()

    textract_response = json.loads(file_bytes)

    # Extraer (y eliminar del dict) el tipo de documento y la llave.
    document_type = textract_response.pop("document_type")
    key = textract_response.pop("key")

    try:
        logger.info(f"[{key}] - Processing results")
        parsed_key = parse_mike_key(key)

        # Configurar el tipo de procesador para el tipo de documento.
        processor = config.PROCESSORS[document_type]()
        processed_results: pd.DataFrame = processor.process_response(textract_response, parsed_key)

        json_results = processed_results.to_json(date_format="iso")

        payload = {
            "key": key,
            "document_type": document_type,
            "data": json_results,
        }

        logger.info(f"[{key}] - Results processed. Returning...")

        s3_client.upload_fileobj(
            io.BytesIO(json.dumps(payload).encode()),
            Bucket=config.S3_BUCKET,
            Key=parsed_mike_key_to_str(parsed_key, new_bandeja="Salida", new_extension="json"),
        )
        return {"key": key, "document_type": document_type, "data": json_results}

    except Exception as e:
        msg = f"[{key}] Unexpected exception in processors: {e}. Event: {event}"
        logging.exception(msg)
        raise Exception(msg)
