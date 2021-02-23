import re
from typing import Any, Dict, List, Union
import datetime
import logging

import pandas as pd

from utils import parse_number, parse_rut
from processor import Processor

MONTHS = {
    'enero': 1,
    'febrero': 2,
    'marzo': 3,
    'abril': 4,
    'mayo': 5,
    'junio': 6,
    'julio': 7,
    'agosto': 8,
    'septiembre': 9,
    'octubre': 10,
    'noviembre': 11,
    'diciembre': 12
}


class PagareTextProcessor():
    def __init__(self, text: str):
        self.text = text.lower()

        # Extraer el texto desde la sección de información del deudor:
        # es decir, desde razón social hasta que termina el pagaré.
        searched = re.search(r'\b(razón social)\b', self.text)
        if searched is not None:
            self.deudor_info_text = self.text[searched.start():]
        else:
            self.deudor_info_text = self.text[5600:]

        self.errors: List[str] = []

    def get_vendedor(self) -> Union[float, None]:
        try:
            pattern = re.compile(r"\bl\w\d{6,8}")
            found = pattern.findall(self.text)

            number = found[0]
            return number.upper()

        except Exception as e:
            logging.error('{} error: {}'.format(
                PagareTextProcessor.get_vendedor.__qualname__, e))
            self.errors.append('Número Vendedor')
            return None

    def get_numero_documento(self) -> Union[float, None]:
        try:
            pattern = re.compile(r"\bn.\s*?\d{7}\b")
            found = pattern.findall(self.text)

            if len(found) > 0:
                return parse_number(found[0])

            # segundo método, menos robusto:
            # else:
            #     pattern = re.compile(r"(?<=n°)(.*?)(?=y)")
            #     found = pattern.findall(self.text)
            #     return parse_number(found[0])
           
           # ultima opcion, sin la N 
            pattern = re.compile(r"\b([12]\d{6})\b") #cualquier numero entre 1000000 y 2999999 rodeado por espacio
            found = pattern.search(self.text)
            return parse_number(found.group(1))
                

        except Exception as e:
            logging.error('{} error: {}'.format(
                PagareTextProcessor.get_numero_documento.__qualname__, e))
            self.errors.append('Numero de Documento')
            return None

    def get_comuna(self) -> Union[float, None]:
        try:
            pattern = re.compile(r"(?<=comuna)(.*?)(?=ciudad)")
            found = pattern.findall(self.text)
            comuna = found[0].replace(':', '').strip().title()
            return comuna
        except Exception as e:
            logging.error('{} error: {}'.format(
                PagareTextProcessor.get_comuna.__qualname__, e))
            self.errors.append('Comuna')
            return None

    def get_direccion(self) -> Union[float, None]:
        try:
            pattern = re.compile(r"(?<=calle)(.*?)(?=comuna)")
            found = pattern.findall(self.text)

            direccion = found[0].replace(':', '').strip().title()
            return direccion.title()
        except Exception as e:
            logging.error('{} error: {}'.format(
                PagareTextProcessor.get_direccion.__qualname__, e))
            self.errors.append('Dirección')
            return None

    def get_nombre(self) -> Union[float, None]:
        try:
            pattern = re.compile(r"(?<=social)(.*?)(?=rut)")
            found = pattern.findall(self.deudor_info_text)
            nombre = found[0]

            # Fix "domiciliado en" aparece producto de una mala detección.
            domiciliado_idx = nombre.find('domiciliado')
            if domiciliado_idx != -1:
                nombre = nombre[0: domiciliado_idx]

            nombre = nombre.replace(':', '').strip().title()
            return nombre.title()
        except Exception as e:
            logging.error('{} error: {}'.format(
                PagareTextProcessor.get_nombre.__qualname__, e))
            self.errors.append('Nombre')
            return None

    def get_fecha_pago_primera_cuota(self) -> Union[datetime.datetime, None]:
        try:
            pattern = re.compile(r"el día \d{1,2} de \w+ de \d{1,4}")
            found = pattern.findall(self.text)

            date_str = found[0]
            date_str = date_str.replace('el día', '').replace(' ', '')
            date_splitted = date_str.split('de')

            day = date_splitted[0]
            month = MONTHS[date_splitted[1]]
            year = date_splitted[2]

            parsed_date = datetime.datetime(year=int(year),
                                            month=int(month),
                                            day=int(day))
            return parsed_date

        except Exception as e:
            logging.error('{} error: {}'.format(
                PagareTextProcessor.get_fecha_pago_primera_cuota.__qualname__, e))
        self.errors.append('Fecha Pago Primera Cuota')
        return None

    def get_dia_de_pago(self) -> Union[float, None]:
        try:
            pattern = re.compile(r"(?<=días)\s*\d{1,2}\s*(?=de)")
            found = pattern.findall(self.text)
            return parse_number(found[0])
        except Exception as e:
            logging.error('{} error: {}'.format(
                PagareTextProcessor.get_dia_de_pago.__qualname__, e))
            self.errors.append('Día de Pago')
            return None

    def get_tasa_interes(self) -> Union[str, None]:
        try:
            pattern = re.compile(r"(?<=de)\s*\d{1,2},\d{1,4}%\s*(?=cada)")
            found = pattern.findall(self.text)

            if len(found) > 0:
                return parse_number(found[0]) / 100

            else:
                pattern = re.compile(r"\d{1,2},\d{1,4}%")
                found = pattern.findall(self.text)
                return parse_number(found[0]) / 100

        except Exception as e:
            logging.error('{} error: {}'.format(
                PagareTextProcessor.get_tasa_interes.__qualname__, e))
            self.errors.append('Tasa Interés')
            return None

    def get_num_cuotas(self) -> Union[float, None]:

        try:
            pattern = re.compile(r"(?<=en)\s*\d{1,3}\s*(?=cuotas)")
            found = pattern.findall(self.text)
            return parse_number(found[0])
        except Exception as e:
            logging.error('{} error: {}'.format(
                PagareTextProcessor.get_num_cuotas.__qualname__, e))
            self.errors.append('Número de Cuotas')
            return None

    def get_representante_legal(self) -> Union[float, None]:
        try:
            # Obtener sección representante legal
            pattern_1 = re.compile(r"(?<=legal)(.+?)(?=domici)")
            representante_section = pattern_1.findall(self.deudor_info_text)[0]

            pattern_2 = re.compile(r"(?<=:)(.+?)(?=rut)")
            found = pattern_2.findall(representante_section)
            nombre = found[0].replace(':', '').strip().title()
            return nombre.title()
        except Exception as e:
            logging.error('{} error: {}'.format(
                PagareTextProcessor.get_representante_legal.__qualname__, e))
            self.errors.append('Representante Legal')
            return None

    def get_rut_representante_legal(self) -> Union[float, None]:
        try:
            # Obtener sección representante legal
            pattern_1 = re.compile(r"(?<=legal)(.+?)(?=domici)")
            representante_section = pattern_1.findall(self.deudor_info_text)[0]

            # Obtener rut
            pattern_2 = re.compile(r"(?<=rut:).+\b")
            found = pattern_2.findall(representante_section)

            return parse_rut(found[0])

        except Exception as e:
            logging.error('{} error: {}'.format(
                PagareTextProcessor.get_rut_representante_legal.__qualname__, e))
            self.errors.append('Rut Representante Legal')
            return None

    def get_rut(self) -> Union[str, None]:
        try:
            # Caso 1: el pagaré tiene representante legal
            rut_pattern_1 = re.compile(r"(?<=rut)(.+?)(?=representante)")
            found = rut_pattern_1.findall(self.deudor_info_text)

            if len(found) > 0:
                # return found[0].split('-')[0].replace(':',
                #                                       '').replace(' ',
                #                                                   '').strip().upper()
                return parse_rut(found[0])

            # Caso 2: El pagaré es de una persona natural
            rut_pattern_2 = re.compile(r"(?<=rut)(.*?)(?=domici)")
            found = rut_pattern_2.findall(self.deudor_info_text)
            # return found[0].split('-')[0].replace(':', '').strip().upper()
            return parse_rut(found[0])

        except Exception as e:
            logging.error('{} error: {}'.format(PagareTextProcessor.get_rut.__qualname__,
                                                e))
            self.errors.append('RUT')
            return None


    def get_fecha_suscripcion(self) -> Union[datetime.datetime, None]:
        try:
            pattern = re.compile(r"santiago, (\d{1,2}) de (\w+) de (\d{4})")
            found = pattern.search(self.text)

            day = found.group(1)
            month = MONTHS[found.group(2)]
            year = found.group(3)

            parsed_date = datetime.datetime(year=int(year),
                                            month=int(month),
                                            day=int(day))
            return parsed_date

        except Exception as e:
            logging.error('{} error: {}'.format(
                PagareTextProcessor.get_fecha_suscripcion.__qualname__, e))
        self.errors.append('Fecha Suscripcion')
        return None

class PagareProcessor(Processor):
    def process_response(self, response: Dict[str, Any], parsed_key: Dict[str, str]) -> pd.DataFrame:

        text = response['text']
        self.processed_text: PagareTextProcessor = PagareTextProcessor(text)

        row = {
            # 'Vendedor': self.processed_text.get_vendedor(),
            'Nombre y Apellido': self.processed_text.get_nombre(),
            'RUT': self.processed_text.get_rut(),
            'Representante': self.processed_text.get_representante_legal(),
            'Rut Representante': self.processed_text.get_rut_representante_legal(),
            'Dirección': self.processed_text.get_direccion(),
            'Comuna': self.processed_text.get_comuna(),
            'Nº Documento': self.processed_text.get_numero_documento(),
            'Nº Cuotas': self.processed_text.get_num_cuotas(),
            'Tasa de Interes': self.processed_text.get_tasa_interes(),
            'fecha Suscrfipción': self.processed_text.get_fecha_suscripcion(),
            'Nombre Documento': parsed_key['filename_full'],
            'Errores': ', '.join(self.processed_text.errors),
            # 'Día de pago': self.processed_text.get_dia_de_pago(),
            # 'Fecha Pago Primera Cuota':
            # self.processed_text.get_fecha_pago_primera_cuota(),
        }
        return pd.DataFrame([row])
