import logging
import os
import io
from typing import Any, Callable, Dict, List, Tuple, Union

import pandas as pd
from sklearn.base import BaseEstimator
from pdf2image import convert_from_bytes

from .commons.textextract_handler import TextExtractHandler, ProcessType
from .commons.response_parser import ResponseParser
from .commons.response_processor import ResponseProcessor
from .forum.desarolllo_contratos import DesarrolloContratosProcessor
from .forum.cav import CAVProcessor
from .forum.pagare import PagareProcessor
from .config import Config as config

logging.basicConfig(level=logging.DEBUG)
"""
Contiene el Pipeline para ejecutar OCR a cualquier archivo de Forum.


Consta de los siguientes pasos:

1. Leer el archivo desde alguna ruta
2. Clasificar el tipo de archivo
3. Analizar el archivo usando el servicio web
4. Parsear los resultados
5. Analizar los resultados y retornar.

"""

FORUM_PROCESSORS = {
    'cav': CAVProcessor,
    'pagare': PagareProcessor,
    'tabla': DesarrolloContratosProcessor
}

FORUM_PROCESS_TYPE_BY_FILE_CLASS = {
    'cav': ProcessType.DETECTION,
    'pagare': ProcessType.DETECTION,
    'tabla': ProcessType.ANALYSIS
}


class DocumentPipeline():
    def __init__(self, file: bytes, filename: str, folder_datetime: str,
                 doc_clf: BaseEstimator, s3_client) -> None:
        self.file = file
        self.filename = filename
        self.folder_datetime = folder_datetime
        self.clf = doc_clf
        self.s3_client = s3_client

    async def analyze_file(
        self,
        role_arn: str,
        bucket: str,
        path: str,
        process_type: int,
        filename: str,
    ) -> List:

        handler = TextExtractHandler(role_arn, bucket, path, process_type, filename)
        handler.CreateTopicandQueue()

        # Esperar a que textextract termine de procesar el documento
        await handler.ProcessDocument()

        handler.DeleteTopicandQueue()
        return handler.responses

    def parse_response(self, raw_response: Dict) -> Tuple[str, Any, Any]:

        # Agregar todas las responses a un solo gran objeto
        response = raw_response[0]
        if len(raw_response) != 0:
            for response_i in raw_response[1:]:
                response['Blocks'] += response_i['Blocks']

        # parsear el gran objeto con los responses
        parsed_response = ResponseParser(response)

        # Extraer text, tablas y formularios desde el response parseado.
        text = ResponseProcessor.get_text(parsed_response)
        tables = ResponseProcessor.get_tables(parsed_response)
        forms = ResponseProcessor.get_forms(parsed_response)

        return text, tables, forms

    def process_response(self, processor: Callable, text: str,
                         tables: Union[List[pd.DataFrame], None],
                         forms: Union[List, Dict, None], filename: str) -> pd.DataFrame:

        processor_instance = processor()
        return processor_instance.process_response(text, tables, forms, filename)

    async def execute_detection(self):
        try:

            logging.info(f'[{self.filename}] - Detection process started')

            # --------------------------------------------------------------------------
            # Cargar el documento como imagen
            pages = convert_from_bytes(self.file)

            # --------------------------------------------------------------------------
            # Subir el documento a S3

            path_in_archivo = os.path.join(config.ARCHIVO_PATH, self.folder_datetime,
                                           self.filename).replace("\\", "/")

            self.s3_client.upload_fileobj(io.BytesIO(self.file), config.S3_BUCKET,
                                          path_in_archivo)

            # --------------------------------------------------------------------------
            # Clasificar el tipo de documento

            self.document_type = self.clf.predict(pages)[0]

            logging.info(
                f'[{self.filename}] - Detected document type: {self.document_type}')
            pages = None  # vaciar variable de los bytes del documento

            # --------------------------------------------------------------------------
            # Escoger el tipo de procesador y tipo de analisis que Textextract ejecutará

            self.processor = FORUM_PROCESSORS[self.document_type]
            self.process_type = FORUM_PROCESS_TYPE_BY_FILE_CLASS[self.document_type]
            logging.info(f'[{self.filename}] - Processor choosed: {self.processor}. '
                         f'TextExtract process_type: {self.process_type}')

            # --------------------------------------------------------------------------
            # Analizar el archivo

            logging.info(f'[{self.filename}] - Detection started')
            self.raw_response = await self.analyze_file(config.ROLE_ARN,
                                                        config.S3_BUCKET,
                                                        path_in_archivo,
                                                        self.process_type, self.filename)

            logging.info('{} - Detection Ended'.format(self.filename))

            # --------------------------------------------------------------------------
            # Parsear resultados

            logging.info(f'[{self.filename}] - Parsing response')
            parsed_responses = self.parse_response(self.raw_response)
            self.text, self.tables, self.forms = parsed_responses

            logging.info('[{}] - Detected {} words aprox. and {} tables'.format(
                self.filename, len(self.text.split(' ')), len(self.tables)))

            # --------------------------------------------------------------------------
            # Analizar respuesta y retornar

            logging.info(f'[{self.filename}] - Processing results')
            results = self.process_response(self.processor, self.text, self.tables,
                                            self.forms, self.filename)
            logging.info(f'[{self.filename}] - Results processed. Returning...')

            return {
                'filename': self.filename,
                'document_type': self.document_type,
                'data': results
            }

        except Exception as e:
            logging.exception(e)
            return e

    def _execute_again_process_response(self, return_dict=False):
        # Método solo para debugear
        try:
            if return_dict:

                return self.process_response(self.processor, self.text, self.tables,
                                             self.forms, self.filename)

                # Analizar respuesta y retornar
            logging.info('{} - Processing results'.format(self.filename))
            results = self.process_response(self.processor, self.text, self.tables,
                                            self.forms, self.filename)
            logging.info('{} - Results processed. Returning...'.format(self.filename))
            return {
                'filename': self.filename,
                'document_type': self.document_type,
                'data': results
            }

        except Exception as e:
            print(f'No se ha ejecutado aún execute_detection. Error: {e}')
