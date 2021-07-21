import io
import json
import logging
from typing import Any, Dict

import cv2
import boto3
import numpy as np
from pdf2image import convert_from_bytes

from src.config import Config as config
from src.utils import MikeKey

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def load_input(input_key: MikeKey, s3_client) -> bytes:
    logger.info(f"[{input_key.to_str()}] Loading file.")

    document_in_s3 = s3_client.get_object(Bucket=config.S3_BUCKET, Key=input_key.to_str())
    document_bytes = document_in_s3["Body"].read()

    logger.info(f"[{input_key.to_str()}] File loaded correctly.")
    return document_bytes


def get_descriptor(img: np.ndarray, input_key: MikeKey, bin_n: int = 16,) -> np.ndarray:
    logger.info(f"[{input_key.to_str()}] Calculating descriptor.")

    gx = cv2.Sobel(img, cv2.CV_32F, 1, 0)
    gy = cv2.Sobel(img, cv2.CV_32F, 0, 1)
    mag, ang = cv2.cartToPolar(gx, gy)
    bins = np.int32(bin_n * ang / (2 * np.pi))  # quantizing binvalues in (0...16)
    bin_cells = bins[:10, :10], bins[10:, :10], bins[:10, 10:], bins[10:, 10:]
    mag_cells = mag[:10, :10], mag[10:, :10], mag[:10, 10:], mag[10:, 10:]
    hists = [np.bincount(b.ravel(), m.ravel(), bin_n) for b, m in zip(bin_cells, mag_cells)]
    hist = np.hstack(hists)  # hist is a 64 bit vector

    logger.info(f"[{input_key.to_str()}] Descriptor calculation successful.")
    return hist


def convert_pdf_to_image(document_bytes: bytes, input_key: MikeKey) -> np.ndarray:
    logger.info(f"[{input_key.to_str()}] Converting PDF to image.")

    pages = convert_from_bytes(document_bytes, first_page=1, last_page=1)

    # Obtener la imagen de la primera página
    first_page = pages[0].convert("RGB")

    # Convertirla a opencv
    image = np.array(first_page)

    # Convertir RGB a BGR
    image = image[:, :, ::-1].copy()

    logger.info(f"[{input_key.to_str()}] Successful PDF conversion.")

    return image


def upload_results(payload: Dict[str, Any], input_key: MikeKey, output_key: MikeKey, s3_client):
    logger.info(f"[{input_key.to_str()}] Payload {payload}")
    logger.info(f"[{input_key.to_str()}] Uploading results to {output_key.to_str()}")

    s3_client.upload_fileobj(
        Fileobj=io.BytesIO(json.dumps(payload, ensure_ascii=False).encode(encoding="UTF-8")),
        Bucket=config.S3_BUCKET,
        Key=output_key.to_str(),
    )
    logger.info(f"[{str(input_key)}] Results uploaded successfully..")


def main(event: dict, context):
    s3_client = boto3.client("s3")
    logger.info("Starting new PDF2Descriptor instance.")
    logger.info(f"Event: {event} | Context: {context}")

    input_raw_key = event["Records"][0]["s3"]["object"]["key"]
    input_key = MikeKey(key=input_raw_key)
    logger.info(f"Current key = {input_key.to_str()}")

    try:

        if input_key.get_extension() == ".pdf":
            # ---------------------------------------------------------------------------------------------------------
            # Cargar input
            document_bytes = load_input(input_key, s3_client)

            # ---------------------------------------------------------------------------------------------------------
            # Obtener imagen
            image = convert_pdf_to_image(document_bytes, input_key)

            # ---------------------------------------------------------------------------------------------------------
            # Obtener HOG
            descriptor = get_descriptor(image, input_key)

            # ---------------------------------------------------------------------------------------------------------
            # Subir resultados.
            output_key = MikeKey(input_key.to_str(new_etapa_intermedio="02_PDF2Descriptor", new_extension="json"))
            payload = {
                "success": True,
                "doc_key": input_key.to_str(),
                "descriptor": descriptor.tolist(),
                "descriptor_type": "HOG",
            }
            upload_results(payload, input_key, output_key, s3_client)

            # ---------------------------------------------------------------------------------------------------------
            # Retornar resultados
            return payload

        else:
            msg = "The provided file is not a pdf."
            raise Exception(msg)

    except Exception as e:
        msg = f"[{input_raw_key}] Exception: {e}"
        error_key = input_key.to_str(new_bandeja="Errores", new_extension=".json")
        logger.exception(msg)

        payload = {
            "step": "PDF2Descriptor",
            "key": input_raw_key,
            "exception": str(e),
            "event": event,
        }

        s3_client.upload_fileobj(
            Fileobj=io.BytesIO(json.dumps(payload, ensure_ascii=False).encode(encoding="UTF-8")),
            Bucket=config.S3_BUCKET,
            Key=error_key,
        )

        raise Exception(msg)
