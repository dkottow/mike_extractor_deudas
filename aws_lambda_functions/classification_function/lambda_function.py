import logging

import numpy as np
from joblib import load

from config import Config as config


def lambda_handler(event, context):

    response_payload = event["responsePayload"]
    key = response_payload["key"]
    descriptor = response_payload["descriptor"]

    logging.info(f"[{key}] Classifing document type")

    try:
        # Cargar el modelo
        clf = load(config.MODEL_PATH)

        # Clasificar
        document_type: str = clf.predict(np.array([descriptor]))[0]

        # Retornar resultados
        return {"key": key, "document_type": document_type}

    except Exception as e:
        msg = f"[{key}] Unexpected exception in document classifier: {e}."
        logging.exception(msg)
        raise Exception(msg)
