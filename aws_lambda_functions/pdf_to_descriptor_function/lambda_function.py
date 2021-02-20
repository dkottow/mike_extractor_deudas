import io
import json
import logging
import os

import cv2
import boto3
import numpy as np

from pdf2image import convert_from_bytes

from config import Config as config
from utils import parse_mike_key

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info(f"{list(os.walk('/opt/lib', topdown = False))}")


def get_descriptor(img: np.ndarray, bin_n: int = 16) -> np.ndarray:
    gx = cv2.Sobel(img, cv2.CV_32F, 1, 0)
    gy = cv2.Sobel(img, cv2.CV_32F, 0, 1)
    mag, ang = cv2.cartToPolar(gx, gy)
    bins = np.int32(bin_n * ang / (2 * np.pi))  # quantizing binvalues in (0...16)
    bin_cells = bins[:10, :10], bins[10:, :10], bins[:10, 10:], bins[10:, 10:]
    mag_cells = mag[:10, :10], mag[10:, :10], mag[:10, 10:], mag[10:, 10:]
    hists = [np.bincount(b.ravel(), m.ravel(), bin_n) for b, m in zip(bin_cells, mag_cells)]
    hist = np.hstack(hists)  # hist is a 64 bit vector
    return hist


def lambda_handler(event: dict, context):

    # Documentar este main
    # Comprobar si el archivo que llega es el zip y no metadata.json u otros...
    # Comprobar

    # key = event.get("key", None)
    key = event["Records"][0]["s3"]["object"]["key"]

    parsed_key = parse_mike_key(key)
    logger.info(f"[{key}] Classifing document type")
    s3 = boto3.client("s3")

    try:

        if parsed_key["extension"] == "pdf":

            # Leer el documento desde s3
            document_in_s3 = s3.get_object(Bucket=config.S3_BUCKET, Key=key)
            document_bytes = document_in_s3["Body"].read()

            logger.info(f"[{key}] Converting pdf to image.")
            pages = convert_from_bytes(document_bytes)

            # Get first page
            first_page = pages[0].convert("RGB")

            # Convert image to opencv
            image = np.array(first_page)

            # Convertir RGB a BGR
            image = image[:, :, ::-1].copy()

            # Obtener HOG
            logger.info(f"[{key}] Calculating descriptor.")
            descriptor = get_descriptor(image)

            # Retornar resultados
            return {'key': key, "descriptor": descriptor.tolist(), "descriptor_type": "HOG"}

        else:
            msg = f"[{key}] Error while trying to classify current file document type: is not a pdf file."
            logger.error(msg)
            raise Exception(msg)

    except Exception as e:
        msg = f"[{key}] Unexpected Exception in pdf to descriptor: {e}."
        logger.exception(msg)
        raise Exception(msg)
