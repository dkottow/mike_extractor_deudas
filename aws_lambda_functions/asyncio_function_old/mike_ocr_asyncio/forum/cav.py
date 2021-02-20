import logging
import re
from typing import Dict, List, Union

import pandas as pd

from ..commons.processor import Processor
from ..commons.utils import parse_rut


class CAVTextProcessor():
    def __init__(self, text: str):
        self.text = text.lower()
        self.errors: List[str] = []

    def get_rut(self) -> Union[str, None]:
        try:
            pattern = re.compile(r"(?<=r.u.n.)(.+?)(?=fec.)")
            found = pattern.findall(self.text)

            if len(found) > 0:
                # return found[0].split('-')[0].replace(':',
                #                                       '').replace(' ',
                #                                                   '').strip().upper()

                return parse_rut(found[0])

            rut_pattern = re.compile(r"\b(\d{1,3}\s*(?:\.\s*\d{1,3}){2}\s*-\s*[\dkK])\b")
            found = rut_pattern.findall(self.text)
            # return found[0].split('-')[0].replace(':', '').replace(' ',
            #                                                        '').strip().upper()
            return parse_rut(found[0])

        except Exception as e:
            logging.error('CAVTextProcessor get_rut error: {}'.format(e))
            self.errors.append('RUT')
            return None

    def get_tipo_vehiculo(self) -> Union[str, None]:
        try:
            pattern = re.compile(r"(?<=vehículo)(.+?)(?=año)")
            found = pattern.findall(self.text)

            return found[0].replace(':', '') \
                           .strip() \
                           .title()

        except Exception as e:
            logging.error('CAVTextProcessor get_tipo_vehiculo error: {}'.format(e))
            self.errors.append('Tipo de Vehículo')
            return None

    def get_marca(self) -> Union[str, None]:
        try:
            pattern = re.compile(r"(?<=marca)(.+?)(?=modelo)")
            found = pattern.findall(self.text)

            return found[0].replace(':', '') \
                           .strip() \
                           .title()

        except Exception as e:
            logging.error('CAVTextProcessor get_marca error: {}'.format(e))
            self.errors.append('Marca')
            return None

    def get_modelo(self) -> Union[str, None]:
        try:
            pattern = re.compile(r"(?<=modelo)(.+?)(?=motor)")
            found = pattern.findall(self.text)

            return found[0].replace(':', '') \
                           .replace('nro.', '')\
                           .replace('nro', '')\
                           .strip() \
                           .title()

        except Exception as e:
            logging.error('CAVTextProcessor get_modelo error: {}'.format(e))
            self.errors.append('Modelo')
            return None

    def get_nro_motor(self) -> Union[str, None]:
        try:
            # Un poco mas específico para evitar que detecte en modelo kia motors
            pattern = re.compile(r"(?<=motor\s)(.+?)(?=chasis)")
            found = pattern.findall(self.text)

            if len(found) > 0:
                return found[0].replace('nro.', '') \
                               .replace(':', '') \
                               .strip() \
                               .upper()

            pattern = re.compile(r"(?<=motor)(.+?)(?=chasis)")
            found = pattern.findall(self.text)
            return found[0].replace('nro.', '') \
                           .replace(':', '') \
                           .strip() \
                           .upper()

        except Exception as e:
            logging.error('CAVTextProcessor get_nro_motor error: {}'.format(e))
            self.errors.append('Modelo')
            return None

    def get_nro_chasis(self) -> Union[str, None]:
        try:

            if 'nro. vin' in self.text:
                pattern = re.compile(r"(?<=chasis)(.+?)(?=vin)")
                found = pattern.findall(self.text)

                return found[0].replace('nro.', '') \
                               .replace(':', '') \
                               .strip() \
                               .upper()

            # TODO: Probar con el otro caso...
            else:
                pattern = re.compile(r"(?<=chasis)(.+?)(?=color)")
                found = pattern.findall(self.text)

                num_chasis = found[0]
                idx_num_serie = num_chasis.find('nro. serie')
                num_chasis = num_chasis[:idx_num_serie]

                return num_chasis.replace(':', '') \
                                 .strip() \
                                 .upper()

        except Exception as e:
            logging.error('CAVTextProcessor get_nro_chasis error: {}'.format(e))
            self.errors.append('Número de Chasis')
            return None

    def get_color(self) -> Union[str, None]:
        try:
            pattern = re.compile(r"(?<=color)(.+?)(?=combustible)")
            found = pattern.findall(self.text)

            return found[0].replace(':', '') \
                           .strip() \
                           .title()

        except Exception as e:
            logging.error('CAVTextProcessor get_color error: {}'.format(e))
            self.errors.append('Color')
            return None

    def get_año(self) -> Union[str, None]:
        try:
            pattern = re.compile(r"(?<=año)(.+?)(?=marca)")
            found = pattern.findall(self.text)

            return found[0].replace(':', '') \
                           .strip()

        except Exception as e:
            logging.error('CAVTextProcessor get_año error: {}'.format(e))
            self.errors.append('Año')
            return None

    def get_n_inscripcion(self) -> Union[str, None]:
        try:
            pattern = re.compile(r"\b\w{1,4}\.\s*\d{1,2}\-\s*\d\b")
            found = pattern.findall(self.text)

            if len(found) != 0:

                return found[0].replace(' ', '').upper()

            pattern = re.compile(r"(?<=inscripción)(.+?)(?=datos)")
            found = pattern.findall(self.text)

            n_inscripcion = found[0].replace(' ', '').replace(':', '').upper()
            n_inscripcion = n_inscripcion.replace('-', '')
            n_inscripcion = n_inscripcion[:-1] + '-' + n_inscripcion[-1]
            return n_inscripcion

        except Exception as e:
            logging.error('CAVTextProcessor get_n_inscripcion error: {}'.format(e))
            self.errors.append('Número Inscripción')
            return None


class CAVProcessor(Processor):
    def process_response(self, text: str, tables: Union[List[pd.DataFrame], None],
                         forms: Union[List, Dict, None], filename: str, *args,
                         **kwargs) -> pd.DataFrame:

        self.processed_text: CAVTextProcessor = CAVTextProcessor(text)

        row = {
            'RUT': self.processed_text.get_rut(),
            'Tipo de Vehículo': self.processed_text.get_tipo_vehiculo(),
            'Marca': self.processed_text.get_marca(),
            'Modelo': self.processed_text.get_modelo(),
            'Nº Motor': self.processed_text.get_nro_motor(),
            'Nº Chassis': self.processed_text.get_nro_chasis(),
            'Color': self.processed_text.get_color(),
            'Año': self.processed_text.get_año(),
            'Nº Inscripción': self.processed_text.get_n_inscripcion(),
            'Archivo CAV': filename,
            'Errores CAV': ', '.join(self.processed_text.errors)
        }
        return pd.DataFrame([row])
