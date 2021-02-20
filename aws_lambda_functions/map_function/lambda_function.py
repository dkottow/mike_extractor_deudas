import io
import json
import logging
from zipfile import ZipFile
import pathlib
import time

import boto3

from config import Config as config
from utils import (
    get_filename,
    get_safe_filename,
    parse_mike_key,
    generate_mike_key,
    get_extension,
    parsed_mike_key_to_str,
)


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: dict, context):

    s3_client = boto3.client("s3")
    logger.info(f"Starting Map function with the event {event}.")
    
    # Obtener la key. Esto nunca debería fallar...
    key = event["Records"][0]["s3"]["object"]["key"]

    try:
        parsed_key = parse_mike_key(key)

        logger.info(f"[{key}] Uploading doc files contained in the input file.")

        if parsed_key["extension"] == "zip":

            # ---------------------------------------------------------------------------------------------------------
            # Obtener el zip desde s3 junto con sus metadatos.

            # Descargar el zip con los documentos.
            compressed_file_s3_obj = s3_client.get_object(Bucket=config.S3_BUCKET, Key=key)
            compressed_file_bytes = compressed_file_s3_obj["Body"].read()

            # Obtener los metadatos del documento.
            compressed_file_head = s3_client.head_object(Bucket=config.S3_BUCKET, Key=key)
            compressed_file_metadata = compressed_file_head["Metadata"]
            email = compressed_file_metadata["email"]

            processable_files = []
            unprocessable_files = []

            # ---------------------------------------------------------------------------------------------------------
            # Descomprimir el zip y subir los archivos.

            with ZipFile(io.BytesIO(compressed_file_bytes)) as zip_archive:

                files_namelist = [path for path in zip_archive.namelist() if pathlib.Path(path).suffix != ""]

                for doc_path in files_namelist:
                    logger.debug(f"[{key}] Extracting {doc_path}.")

                    filename = get_safe_filename(get_filename(doc_path))
                    extension = get_extension(doc_path)

                    # Generar key acorde a la convención del proyecto
                    doc_key = generate_mike_key(
                        bandeja="Archivo",
                        datetime=parsed_key["datetime"],
                        empresa="Forum",
                        filename=filename,
                        extension=extension,
                    )

                    if extension == "pdf":

                        # Abrir el archivo
                        file_bytes = zip_archive.open(doc_path).read()

                        # Subir el documento a S3
                        s3_client.upload_fileobj(io.BytesIO(file_bytes), config.S3_BUCKET, doc_key)
                        logger.info(f"[{key}] Document {doc_path} was successfully uploaded to s3.")

                        processable_files.append(doc_key)
                        # Dormir 1 segundo por cada archivo para no alcanzar los límites de llamada de AWS
                        # (máximo 4 textract concurrentes...)
                        time.sleep(1)

                    else:

                        # Manejar el caso en que un documento no sea pdf.
                        msg = f"{doc_path} no es un documento pdf."
                        s3_client.upload_fileobj(
                            io.BytesIO(json.dumps({"success": False, "error": msg}).encode()),
                            Bucket=config.S3_BUCKET,
                            Key=parsed_mike_key_to_str(parsed_key, new_bandeja="Errores", new_extension=".json"),
                        )
                        logger.error(f"[{key}] {doc_path} is not a pdf file.")
                        unprocessable_files.append(doc_path)

            # ---------------------------------------------------------------------------------------------------------
            # Encolar los parámetros para el reduce.

            reduce_payload = {
                "processable_files": processable_files,
                "unprocessable_files": unprocessable_files,
                "empresa": "Forum",
                "folder_datetime": parsed_key["datetime"],
                "email": email,
                "number_of_retries": 0,
            }

            logging.info(f"[{key}] Call to reduce successfully queued with the following payload: {reduce_payload}.")
            return {"success": True, **reduce_payload}

        else:
            # Este caso no debería ocurrir nunca en el front.
            # Sin embargo, debería de todas maneras producir un email con alguna respuesta...
            # TODO: Terminar este else con el envio del correo.
            msg = f"[{key}] No zip file founded on the uploaded file."
            logger.exception(msg)
            s3_client.upload_fileobj(
                io.BytesIO(json.dumps({"success": False, "error": msg}).encode()),
                Bucket=config.S3_BUCKET,
                Key=parsed_mike_key_to_str(parsed_key, new_bandeja="Errores", new_extension=".json"),
            )
            raise Exception(msg)

    except Exception as e:
        msg = f"[{key}] Unexpected Exception: {e}"
        raise Exception(msg)
