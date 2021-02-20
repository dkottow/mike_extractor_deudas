import io
import json
import logging

import boto3

from textract_adapter import TextractClientAdapter
from config import Config as config
from utils import parse_mike_key, parsed_mike_key_to_str

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: dict, context):
    # Parsear parametros de la llamada del lambda.
    response_payload = event["responsePayload"]

    # textextract_response = response_payload[""]
    key = response_payload["key"]
    document_type = response_payload["document_type"]

    parsed_key = parse_mike_key(key)

    logger.info(f"Classifing {key} document type")
    s3_client = boto3.client("s3")

    try:

        process_type = config.PROCESS_TYPE_BY_FILE_CLASS[document_type]
        logger.info(f"[{key}] - Textract analysis started")

        textract_adapter = TextractClientAdapter(key, process_type)
        textract_adapter.CreateTopicandQueue()

        # Esperar a que textextract termine de procesar el documento
        textract_adapter.ProcessDocument()

        # Limpiar la cola
        textract_adapter.DeleteTopicandQueue()

        # Obtener los resultados parseados.
        textract_response = textract_adapter.GetResults(parsed=True)

        # Agregar la key y el tipo de documento al json.
        textract_response["key"] = key
        textract_response["document_type"] = document_type

        # Guardar el json en S3
        textract_response_key = parsed_mike_key_to_str(parsed_key, new_bandeja="Archivo", new_extension="json")
        s3_client.upload_fileobj(
            io.BytesIO(json.dumps(textract_response).encode(encoding='UTF-8')), config.S3_BUCKET, textract_response_key
        )

        logger.info(
            f"[{key}] - Detection Ended. Found {len(textract_response['text'].split(' '))} words "
            f"aprox. and {len(textract_response['tables'])} tables"
        )
        return {**textract_response}

    except Exception as e:
        msg = f"[{key}] Unexpected exception in Textract Adapter: {e}. Event: {event}"
        raise Exception(msg)
