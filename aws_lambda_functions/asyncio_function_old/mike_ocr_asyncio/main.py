import io
import os
import asyncio
import logging
from typing import Dict, List, Union
from zipfile import ZipFile

import pandas as pd
import boto3
from sklearn.feature_extraction.text import strip_accents_ascii

from .forum_pipeline import DocumentPipeline as ForumDocumentPipeline
from .forum.forum_document_classifier import ForumDocumentClassifier
from .config import Config as config

ses = boto3.client("ses")
s3 = boto3.client("s3")

logging.basicConfig(format="[%(levelname)s] %(asctime)s - %(message)s", level=logging.INFO)


def get_safe_name(key: str) -> str:
    name = key.split("/")[-1]
    return strip_accents_ascii(name.lower().strip())


def get_name(key: str) -> str:
    return key.split("/")[-1]


def get_file_extension(key: str) -> str:
    return key.split(".")[-1]


def get_not_processed_files(merged_results: pd.DataFrame, file_list: List[str]) -> List[str]:

    processed_files = merged_results.loc[:, ["Archivo Desarrollo Contratos", "Archivo Pagare", "Archivo CAV"]]

    processed_files = processed_files.values
    processed_files = processed_files.flatten()
    processed_files = [str(file).lower() for file in processed_files]

    not_found_files = []
    for file in file_list:
        filename = get_name(file)
        if filename not in processed_files:
            not_found_files.append(filename)

    return not_found_files


def merge_results(
    results: List[Dict[str, Union[str, pd.DataFrame]]],
    document_types: List[str],
    cols_required_by_doc_types=Dict[str, str],
) -> pd.DataFrame:

    try:

        results_filtered = [result for result in results if isinstance(result, dict)]

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

        results_by_type_concatenated = {k: pd.DataFrame(columns=cols_required_by_doc_types[k]) for k in document_types}
        for results_type, results in results_by_type.items():
            if len(results) > 0:
                results_by_type_concatenated[results_type] = pd.concat(results).dropna(subset=["RUT"])

        # ------------------------------------------------------------------------------
        # Mergear los df concatenados en el paso anterior
        # TODO: Generalizar los merge...

        results_final = (
            results_by_type_concatenated["cav"]
            .merge(results_by_type_concatenated["pagare"], on="RUT", how="outer")
            .merge(results_by_type_concatenated["tabla"], on="RUT", how="outer")
        )

        # Retornar
        return results_final

    except Exception as e:
        logging.exception(
            f"Exception raised while trying to process results. "
            f"Error: {e}, Results: {results}. Returning empty df..."
        )
        return pd.DataFrame()


async def main(key: str) -> Dict:
    # Documentar este main
    # Comprobar si el archivo que llega es el zip y no metadata.json u otros...
    # Comprobar
    logging.info(f"Executing OCR detecton on the file {key}")

    filename = get_name(key)
    file_extension = get_file_extension(key)

    forum_doc_clf = ForumDocumentClassifier(config.PATH_TO_MODEL)

    if file_extension == "zip":

        compressed_file_s3_obj = s3.get_object(Bucket=config.S3_BUCKET, Key=key)
        compressed_file_bytes = compressed_file_s3_obj["Body"].read()

        compressed_file_head = s3.head_object(Bucket=config.S3_BUCKET, Key=key)
        compressed_file_metadata = compressed_file_head["Metadata"]
        email = compressed_file_metadata["email"]
        folder_datetime = compressed_file_metadata["folder_datetime"]

        pipelines = {}

        processable_files = []
        unprocessable_files = []

        with ZipFile(io.BytesIO(compressed_file_bytes)) as zip_archive:

            for doc in zip_archive.namelist():
                logging.info(f"Extracting {doc}")

                if get_file_extension(doc) == "pdf":

                    file_bytes = zip_archive.open(doc).read()
                    pipelines[get_safe_name(doc)] = ForumDocumentPipeline(
                        file=file_bytes,
                        filename=get_name(doc),
                        folder_datetime=folder_datetime,
                        doc_clf=forum_doc_clf,
                        s3_client=s3,
                    )
                    processable_files.append(doc)

                else:
                    unprocessable_files.append(doc)

        tasks = [pipeline.execute_detection() for pipeline in pipelines.values()]
        results = await asyncio.gather(*tasks)

        # Concatenar y joinear resultados
        merged_results_df = merge_results(
            results=results,
            document_types=config.FORUM_DOCUMENT_TYPES,
            cols_required_by_doc_types=config.COLS_REQUIRED_BY_DOC_TYPES,
        )

        # Reordenar columnas similar al formato original
        merged_results_df = merged_results_df.loc[:, config.COL_ORDER]

        # Obtener los archivos que no pudieron ser procesados
        not_processed_files = get_not_processed_files(merged_results_df, processable_files + unprocessable_files)

        not_processed_files_df = pd.DataFrame(not_processed_files, columns=["Archivos que no pudieron ser procesados"])

        with io.BytesIO() as output:
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                merged_results_df.to_excel(writer, sheet_name="Resultados OCR")
                not_processed_files_df.to_excel(writer, sheet_name="Errores")
                writer.save()

                path_resultados = os.path.join(
                    config.SALIDA_PATH, folder_datetime, f"Resultados_OCR_{folder_datetime}.xlsx"
                ).replace("\\", "/")

                data = output.getvalue()
        s3.upload_fileobj(io.BytesIO(data), Bucket=config.S3_BUCKET, Key=path_resultados)

        # TODO: Agregar referencia a errores en S3
        # TODO: Enviar un email con el archivo de resultados
        return {"success": True}

    else:
        return {"success": False, "error": f"No zip file founded. File requested: {key}"}
