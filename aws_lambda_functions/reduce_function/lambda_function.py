import io
import os
import json
import logging
from typing import Any, Dict, List, Tuple, Union
from functools import reduce
from collections import defaultdict

import boto3
import numpy as np
import pandas as pd
from botocore.exceptions import ClientError
from pathlib import Path

from config import Config as config
from utils import get_extension

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_name(key: str) -> str:
    if isinstance(key, str):
        return Path(key).stem
    return None


def load_results(
    salida_keys: List[str], s3_client, incorrectly_processed_files
) -> Tuple[List[Any], List[str], List[str]]:
    """Lee y parsea cada uno de los archivos de resultados"""

    loaded_results = []
    successfully_processed_files = []

    logger.info("Reading results files.")
    for key in salida_keys:
        try:
            obj = s3_client.get_object(Bucket=config.S3_BUCKET, Key=key)
            result = json.loads(obj["Body"].read().decode())

            # Convertir los json de los resultados a dataframes
            data_df = pd.read_json(result["data"])

            # Convertir fechas ISO a dates.
            data_df = data_df.apply(
                lambda col: pd.to_datetime(col, errors="ignore") if col.dtypes == object else col, axis=0
            )
            # Eliminar timezones (para poder guardar las fechas en el excel) y dejar solo fechas (sin horas)
            for col in data_df.select_dtypes(["datetimetz"]).columns:
                data_df[col] = data_df[col].dt.tz_convert(None)
                data_df[col] = data_df[col].dt.date

            result["data"] = data_df
            successfully_processed_files.append(key)
            loaded_results.append(result)

        except Exception as e:
            logger.exception("Exception while reading {key}: {e}")
            incorrectly_processed_files.append(key)

    logger.info(f"Processable files: {successfully_processed_files}")
    logger.info(f"Unprocessable files: {incorrectly_processed_files}")

    return loaded_results, successfully_processed_files, incorrectly_processed_files


def merge_results(
    loaded_results: List[Dict[str, Union[str, pd.DataFrame]]],
) -> pd.DataFrame:
    """Mergea los resultados en un único Dataframe"""

    document_types: List[str] = config.DOCUMENT_TYPES

    detection_columns: Dict[str, Dict[str, dict]] = config.DETECTION_COLUMNS

    required_cols_by_doc_type = defaultdict(list)
    for doc_type, cols_by_doc_type in detection_columns.items():
        for col_name, col_desc in cols_by_doc_type.items():
            if col_desc["required"] is True:
                required_cols_by_doc_type[doc_type].append(col_name)
    try:

        results_filtered = [result for result in loaded_results if isinstance(result, dict)]

        # ------------------------------------------------------------------------------
        # Separar los resultados por tipo de documento y guardarlos en un diccionario

        results_by_type: Dict[str, list] = {k: [] for k in document_types}
        for result in results_filtered:

            data = result["data"]
            document_type = result["document_type"]

            if isinstance(data, pd.DataFrame) and document_type in document_types:
                results_by_type[result["document_type"]].append(data)

        # ------------------------------------------------------------------------------
        # Concatenar

        # Es decir, unir todos los cav en un df, todos las tablas en otro df y así...
        # En el caso que no se haya detectado ningun tipo de documento para un cierto
        # tipo, se genera un dataframe con las columnas por defecto de la salida.

        results_by_type_concatenated = {
            doc_type: pd.DataFrame(columns=required_cols_by_doc_type[doc_type]) for doc_type in document_types
        }
        for results_type, results in results_by_type.items():
            if len(results) > 0:
                results_by_type_concatenated[results_type] = pd.concat(results).dropna(subset=["RUT"])

        # ------------------------------------------------------------------------------
        # Mergear los df concatenados en el paso anterior

        # El merge es un left join con respecto al pagaré.
        # Es decir, requiere que el pagaré esté completo.

        pagare_df = results_by_type_concatenated["pagare"]
        tabla_df = results_by_type_concatenated["tabla"]
        cav_df = results_by_type_concatenated["cav"]

        merged_df = pd.merge(pagare_df, tabla_df, on="RUT", how="left")
        merged_df = pd.merge(merged_df, cav_df, on="RUT", how="left")

        # Retornar
        return merged_df

    except Exception as e:
        raise Exception(f"Step: Merge results. Exception: {e}.")


def postprocess_merge_results(merged_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Una mezcla de varias operaciones para adaptar merged_df al formato de salida."""

    # Columnas requeridas en la página de resultados.
    complete_and_partial_columns = config.COMPLETE_AND_PARTIAL_COLS
    # Columnas con valores por defecto
    cols_with_default_value = config.DEFAULT_VALUES

    # Columnas requeridas para verificar completitud:
    detection_columns: Dict[str, Dict[str, dict]] = config.DETECTION_COLUMNS

    requeried_cols = []
    for cols_by_doc_type in detection_columns.values():
        for col_name, col_desc in cols_by_doc_type.items():
            if col_desc["required"] is True and col_desc["output"] is True:
                requeried_cols.append(col_name)

    try:

        # Crear las columnas faltantes en los resultados.
        for col in complete_and_partial_columns:
            if col not in merged_df.columns:
                merged_df[col] = ""

        # Agregar los valores por defecto de las columnas que lo requieran
        for default_col_name, default_col_value in cols_with_default_value.items():
            merged_df[default_col_name] = default_col_value

        # Separar los resultados en completos y parciales.
        only_required_values = merged_df.loc[:, requeried_cols]
        num_of_nan_per_row = only_required_values.isnull().sum(axis=1)
        complete_results_df = merged_df.loc[num_of_nan_per_row == 0, :]
        partial_results_df = merged_df.loc[num_of_nan_per_row != 0, :]

        return complete_results_df, partial_results_df

    except Exception as e:
        raise Exception(f"Step: Postprocess. Exception: {e}.")


def get_requiered_columns_by_document_type() -> Dict[str, List[str]]:

    # Especificación de las columnas de la detección.
    detection_columns: Dict[str, Any] = config.DETECTION_COLUMNS

    # Obtener las columnas requeridas según tipo de documento
    required_cols_by_doc_types: Dict[str, List[str]] = defaultdict(list)
    for doc_type, cols_by_doc_types in detection_columns.items():
        for col_name, col_desc in cols_by_doc_types.items():
            required: bool = col_desc["required"]
            if required is True:
                required_cols_by_doc_types[doc_type].append(col_name)

    return required_cols_by_doc_types


def get_processed_docs_df(
    loaded_results: List[Dict[str, Union[str, pd.DataFrame]]],
    file_list: List[str],
    complete_results_df: pd.DataFrame,
    partial_results_df: pd.DataFrame,
) -> pd.DataFrame:

    # Columnas requeridas en la página de documentos procesados y en la página de resultados completos y parciales.
    processed_documents_columns = config.PROCESSED_DOCS_COLS
    complete_and_partial_columns = config.COMPLETE_AND_PARTIAL_COLS
    all_columns = processed_documents_columns + complete_and_partial_columns

    # Valores por defecto para las columnas
    columns_with_default_values = config.DEFAULT_VALUES
    # Columnas de la entrada marcadas como requeridas
    required_cols_by_doc_types = get_requiered_columns_by_document_type()

    processed_docs_df = pd.DataFrame([], columns=processed_documents_columns)
    for loaded_result in loaded_results:
        current_data: pd.DataFrame = loaded_result["data"]
        current_document_type: str = loaded_result["document_type"]
        current_document_required_cols = required_cols_by_doc_types[current_document_type]

        # Contar el número de nans de cada detección.
        num_of_nan_per_row = current_data.loc[:, current_document_required_cols].isnull().sum(axis=1)

        if num_of_nan_per_row.values[0] == 0:
            current_data["Estado Procesamiento"] = "Exitoso"
            current_data["Tipo de error"] = None
        else:
            current_data["Estado Procesamiento"] = "Parcialmente exitoso"
            current_data["Tipo de error"] = f'Campos faltantes: {current_data["Errores"]}'

        processed_docs_df = pd.concat([processed_docs_df, current_data])

    # Crear las columnas faltantes en los resultados.
    for col in all_columns:
        if col not in processed_docs_df.columns:
            processed_docs_df[col] = ""

    # Agregar los valores por defecto de las columnas que lo requieran
    for default_col_name, default_col_value in columns_with_default_values.items():
        processed_docs_df[default_col_name] = default_col_value

    # Obtener los archivos procesados
    processed_files = processed_docs_df["Nombre Documento"].values.flatten()

    unprocessed_files = []
    processed_files = [get_name(key) for key in processed_files if key is not None]

    for key in file_list:
        current_filename = get_name(key)
        if (
            current_filename is not None
            and current_filename not in processed_files
            and "Resultados_OCR_" not in current_filename
        ):
            unprocessed_files.append(current_filename)

    # Reportar como Error de detección todos los archivos que se hayan enviado para detección en el map pero que no se
    # encuentren en los resultaods
    for missing_file in unprocessed_files:
        d = {
            "Nombre Documento": [missing_file + ".pdf"],
            "Estado Procesamiento": ["Fallido"],
            "Tipo de Error": ["Error de detección"],
        }
        processed_docs_df = pd.concat([processed_docs_df, pd.DataFrame(d)])

    # Obtener todos los archivos que fueron usados en el df de resultados completos.
    files_in_complete_results = []
    for col in complete_results_df.columns:
        if "Nombre Documento" in col:
            filenames = complete_results_df.loc[:, col].dropna().tolist()
            for filename in filenames:
                files_in_complete_results.append(get_name(filename))

    # Obtener todos los archivo que no fueron usados en el df de resultados completo ni que estén en
    # los documentos no procesados por algún motivo.
    files_not_in_complete_results = []
    for key in file_list:
        filename = get_name(key)
        if filename not in unprocessed_files and filename not in files_in_complete_results:
            files_not_in_complete_results.append(f"{filename}.pdf")

    # Indicar que todos los archivos que no estén en resultados completados sean dados como fallidos.
    processed_docs_df = processed_docs_df.reset_index(drop=True)
    for partial_detected_file in files_not_in_complete_results:
        try:
            # Buscar la fila del documento
            row = processed_docs_df[processed_docs_df["Nombre Documento"] == partial_detected_file]
            # Buscar el indice
            idx = row.index.values[0]
            # Agregar el error
            processed_docs_df.at[idx, "Estado Procesamiento"] = "Fallido"
            processed_docs_df.at[idx, "Tipo de Error"] = "Cruce RUT"
        except Exception as e:
            logger.exception(f"Error while trying to report incomplete detection file {partial_detected_file}: {e}")

    processed_docs_df = processed_docs_df.sort_values(by="Nombre Documento")

    return processed_docs_df.loc[:, all_columns]


def write_excel(
    complete_results_df: pd.DataFrame,
    partial_results_df: pd.DataFrame,
    processed_docs_df: pd.DataFrame,
    empresa: str,
    folder_datetime: str,
    s3_client,
) -> str:
    """Escribe el excel con los resultados en S3"""

    try:
        dataframes = {
            "Detecciones Completas": complete_results_df,
            "Detecciones Parciales": partial_results_df,
            "Documentos Procesados": processed_docs_df,
        }
        # Abrir un buffer virtual
        with io.BytesIO() as output:
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:

                for sheetname, df in dataframes.items():  # loop through `dict` of dataframes
                    df.to_excel(writer, sheet_name=sheetname, index=False)  # send df to writer
                    worksheet = writer.sheets[sheetname]  # pull worksheet object
                    for idx, col in enumerate(df):  # loop through all columns
                        series = df[col]
                        max_len = (
                            max(
                                (
                                    series.astype(str).map(len).max(),  # len of largest item
                                    len(str(series.name)),  # len of column name/header
                                )
                            )
                            + 1
                        )  # adding a little extra space
                        worksheet.set_column(idx, idx, max_len)  # set column width
                writer.save()

                filename = f"Resultados_deteccion_{folder_datetime}.xlsx"
                path_resultados = os.path.join(config.SALIDA_PATH, empresa, folder_datetime, filename)
                data = output.getvalue()

        logging.info("Uploading excel with the results to {path_resultados}")
        s3_client.upload_fileobj(
            io.BytesIO(data), Bucket=config.S3_BUCKET, Key=path_resultados, ExtraArgs={"ACL": "public-read"}
        )
        results_url = f"https://{config.S3_BUCKET}.s3.{config.AWS_REGION}.amazonaws.com/{path_resultados}"
        return results_url

    except Exception as e:
        raise Exception(f"Step: Write results. Exception: {e}.")


def send_email(recipient: str, body: str):
    """Envia el email de notificación."""

    # The HTML body of the email.

    # Create a new SES resource and specify a region.
    client = boto3.client("ses", region_name=config.AWS_REGION)
    logging.info(f"Sending mail to {recipient}")

    # Try to send the email.
    try:
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                "ToAddresses": [
                    recipient,
                ],
            },
            Message={
                "Body": {
                    "Html": {
                        "Charset": config.EMAIL_CHARSET,
                        "Data": body,
                    },
                    "Text": {
                        "Charset": config.EMAIL_CHARSET,
                        "Data": body,
                    },
                },
                "Subject": {
                    "Charset": config.EMAIL_CHARSET,
                    "Data": config.EMAIL_SUBJECT,
                },
            },
            Source=config.EMAIL_SENDER,
        )
    # Display an error if something goes wrong.
    except ClientError as e:
        logger.exception(e.response["Error"]["Message"])
    else:
        logger.info("Email sent!")


def flat_list(t: List) -> List:
    return [item for sublist in t for item in sublist]


def lambda_handler(event, context) -> Dict:

    s3 = boto3.client("s3")
    sqs = boto3.client("sqs")
    logger.info(f"Reduce started. Event: {event}")

    # ----------------------------------------------------------------------------------------------------------------
    # Procesamiento inicial de los parámetros de entrada.

    # Eliminar el mensaje de la cola
    receipt_handle = event["Records"][0]["receiptHandle"]
    try:
        sqs.delete_message(QueueUrl=config.SQS_URL, ReceiptHandle=receipt_handle)
    except Exception as e:
        logger.warning(
            f"Error while trying to delete message: receipt_handle not found in the queue: {receipt_handle}. "
            f"Exception: ({e})"
        )

    # Parsear el payload
    parsed_body = json.loads(event["Records"][0]["body"])
    # Caso 1: La función fue invocada desde el destinity del map function (primera invocación).
    if "responsePayload" in parsed_body:
        payload = parsed_body["responsePayload"]
    # Caso 2: La función fue invocada desde una llamada desde el lambda (segunda invocación en adelante):
    else:
        payload = parsed_body

    # Extraer los datos desde el payload
    folder_datetime: str = payload["folder_datetime"]
    empresa: str = payload["empresa"]
    email: str = payload["email"]

    processable_files: List[str] = payload["processable_files"]
    unprocessable_files: List[str] = payload["unprocessable_files"]

    number_of_retries: int = payload["number_of_retries"]

    logger.info(f"Current number of retries: {number_of_retries + 1} / {config.MAX_RETRY_ATTEMPTS}")

    try:

        # ------------------------------------------------------------------------------------------------------------
        # Obtener el nombre de los resultados y verificar que todos los mapeos hayan terminado.

        # Obtener la key de los resultados de los analisis.
        try:
            salida_objects = s3.list_objects_v2(
                Bucket=config.S3_BUCKET, Prefix=os.path.join(config.SALIDA_PATH, empresa, folder_datetime)
            )
            salida_keys = [obj["Key"] for obj in salida_objects["Contents"] if get_extension(obj["Key"]) == "json"]
        except Exception as e:
            salida_keys = []

        # Obtener la key de los archivos que levantaron un error al ser analizados durante el proceso de mapeo.
        try:
            errores_objects = s3.list_objects_v2(
                Bucket=config.S3_BUCKET, Prefix=os.path.join(config.ERRORES_PATH, empresa, folder_datetime)
            )
            errores_keys = [obj["Key"] for obj in errores_objects["Contents"]]
        except Exception as e:
            errores_keys = []

        # Totalidad de los archivos json encontrados en las bandejas de salida y de errores.
        salida_errores_keys = salida_keys + errores_keys
        len_salida_errores_keys = len(salida_errores_keys)

        # En este punto se decide si se continua con el reduce en el (caso que se hayan procesado todos los archivos o
        # que se hayan alcanzado el máximo de retries) o si se programa una nueva ejecución de este.
        #
        # Para detener aquí la ejecución del reduce y encolar una nueva ejecución, verifico que se cumplan las
        # siguientes condiciones:
        #
        # 1. la cantidad de archivos procesados (ya sea exitosamente o con error) sea menor a la cantidad total de
        #    archivos procesables indicada por el map (en el parámetro payload).
        # 2. la cantidad de reintentos es menor que el máximo de reintentos permitido.

        logger.info(
            f"Number of expected files indicated by the map function: {len(processable_files)}, "
            f"Number of found results files: {len_salida_errores_keys}"
        )
        logger.info(f"Expected files indicated by the map function: {processable_files}")
        logger.info(f"Found results files: {salida_errores_keys}")

        if len_salida_errores_keys < len(processable_files) and number_of_retries < config.MAX_RETRY_ATTEMPTS:
            payload["number_of_retries"] += 1
            sqs.send_message(QueueUrl=config.SQS_URL, MessageBody=json.dumps(payload))
            logging.info("Pausing reduce execution due to a missing results.")
            return {
                "success": True,
                "email_sent": False,
                "results_file": None,
                "enqueued_execution": True,
            }

        # ----------------------------------------------------------------------------------------------------------------
        # Carga de los datos desde s3

        # Arreglo con los archivos que no eran pdf mas los que levantaron algun error durante el mapeo.
        incorrectly_processed_files = unprocessable_files + errores_keys

        # Leer los resultados obtenidos en el map desde s3
        logger.info("Reading results files from s3.")
        loaded_results, successfully_processed_files, incorrectly_processed_files = load_results(
            salida_keys=salida_keys, incorrectly_processed_files=incorrectly_processed_files, s3_client=s3
        )

        # -------------------------------------------------------------------------------------------------------------
        # Concatenar y joinear resultados
        logger.info("Merging results.")
        merged_results_df = merge_results(loaded_results)

        # -------------------------------------------------------------------------------------------------------------
        # Ejecutar el postprocesamiento de los merged results
        logger.info("Executing postprocessing.")
        complete_results_df, partial_results_df = postprocess_merge_results(merged_results_df)

        # -------------------------------------------------------------------------------------------------------------
        # Obtener el df del detalle del procesamiento por archivo.
        logger.info("Getting Processed Docs.")
        file_list = successfully_processed_files + incorrectly_processed_files
        processed_docs_df = get_processed_docs_df(loaded_results, file_list, complete_results_df, partial_results_df)

        # Reordenar columnas similar al formato original
        complete_and_partial_columns = config.COMPLETE_AND_PARTIAL_COLS
        complete_results_df = complete_results_df.loc[:, complete_and_partial_columns]
        partial_results_df = partial_results_df.loc[:, complete_and_partial_columns]

        # -------------------------------------------------------------------------------------------------------------
        # Escribir el excel de los resultados a s3
        logger.info("Writing results.")
        results_url = write_excel(
            complete_results_df, partial_results_df, processed_docs_df, empresa, folder_datetime, s3
        )
        email_body = config.SUCCESS_EMAIL_BODY_HTML.format(results_url)
        send_email(email, email_body)

        return {
            "success": True,
            "results_file": results_url,
            "email_sent": True,
            "enqueued_execution": False,
        }

    # ----------------------------------------------------------------------------------------------------------------
    # Manejo de errores: Enviar un email en caso de que alguna parte de este proceso falle.

    except Exception as e:
        logger.exception(f"Exception raised while executing reduce. {e}")
        email_body = config.ERROR_EMAIL_BODY_HTML
        send_email(email, email_body)

        return {"success": False, "email_sent": True, "enqueued_execution": False, "exception": str(e)}
