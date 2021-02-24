import datetime
import logging
import re
from typing import Any, Dict, Union, List

import pandas as pd
from dateutil.parser import parse as parse_date

from processor import Processor
from utils import standardize_word, concatenar_fila, is_date, parse_number, parse_rut


class DDATablesProcessor:
    def __init__(self, tablas_pagos: List[pd.DataFrame]):

        if len(tablas_pagos) == 0:
            tabla_pagos = pd.DataFrame()

        elif len(tablas_pagos) > 1:
            tabla_pagos = pd.concat(tablas_pagos, axis=0)
        else:
            tabla_pagos = tablas_pagos[0]

        tabla_pagos = self.encontrar_encabezado(tabla_pagos)
        tabla_pagos = self.eliminar_fila_total(tabla_pagos)

        self.tabla_pagos = tabla_pagos
        self.fila_ultima_cuota_pagada = self.get_fila_ultima_cuota_pagada()
        self.fila_primera_cuota_impaga = self.get_fila_primera_cuota_impaga()
        self.fila_cuoton = self.get_fila_cuoton()

        self.errors: List[str] = []

    def encontrar_encabezado(self, tabla_pagos):
        tabla_pagos = tabla_pagos.copy()
        # Palabras que tiene que contener una fila para ser el encabezado.
        headers_a_buscar = ["cuota", "estado"]

        for idx, row in tabla_pagos.iterrows():
            # Convertir la fila en string
            fila_como_str = concatenar_fila(row)

            # Si encuentro una fila que contenga las palabras de headers_a_buscar
            # la transformo a header y elimino todas las filas anteriores.
            if all([header in fila_como_str for header in headers_a_buscar]):
                columns = tabla_pagos.iloc[0, :]
                columns = [standardize_word(col) for col in columns]

                # Fijar nombres de las columnas
                tabla_pagos.columns = [
                    "cuota",
                    "fvcto",
                    "factura",
                    "estado",
                    "fecha_pago",
                    "valor_cuota",
                    "monto_actual",
                    "amortiz",
                    "intereses",
                    "saldo_capital",
                ]
                tabla_pagos = tabla_pagos.iloc[1:, :]
                # Reiniciar índices
                tabla_pagos = tabla_pagos.reset_index(drop=True)
                break

        return tabla_pagos

    def eliminar_fila_total_old(self, tabla_pagos):
        tabla_pagos = tabla_pagos.copy()

        palabras_a_buscar = ["totales", "tot", "t o t a l e s", "t o t"]

        for idx, row in tabla_pagos.iterrows():
            # Convertir la fila en string
            fila_como_str = concatenar_fila(row)

            # Si encuentro una fila que contenga alguna de las palabras de palabras_a_buscar,
            # elimino esta y todas las que vengan.
            if any([palabra in fila_como_str for palabra in palabras_a_buscar]):
                tabla_pagos = tabla_pagos.iloc[0:idx, :]
                break
        return tabla_pagos

    def eliminar_fila_total(self, tabla_pagos):
        tabla_pagos = tabla_pagos.copy()
        for idx, row in tabla_pagos.iterrows():

            # Si encuentro una fila que tenga la primera columna vacia,
            # elimino esta y todas las que vengan.
            if row["cuota"].strip() == "":
                tabla_pagos = tabla_pagos.iloc[0:idx, :]
                break
        return tabla_pagos

    # Obtener filas

    def get_fila_ultima_cuota_pagada(self) -> Union[pd.Series, None]:
        try:
            for idx, row in self.tabla_pagos.iterrows():
                if not is_date(row["fecha_pago"]):
                    if idx == 0:
                        return None

                    return self.tabla_pagos.loc[idx - 1, :]
            return None
        except Exception as e:
            logging.error("{} error: {}".format(DDATablesProcessor.get_fila_ultima_cuota_pagada.__qualname__, e))
            return None

    def get_fila_primera_cuota_impaga(self) -> Union[pd.Series, None]:

        try:
            for idx, row in self.tabla_pagos.iterrows():
                if not is_date(row["fecha_pago"]):
                    return self.tabla_pagos.loc[idx, :]
            return None
        except Exception as e:
            logging.error("{} error: {}".format(DDATablesProcessor.get_fila_primera_cuota_impaga.__qualname__, e))
            return None

    def get_fila_cuoton(self) -> Union[pd.Series, None]:

        try:
            return self.tabla_pagos.tail(1)
        except Exception as e:
            logging.error("{} error: {}".format(DDATablesProcessor.get_fila_cuoton.__qualname__, e))
            return None

    # Obtener valores
    def get_ultima_cuota_pagada(self) -> Union[int, None]:
        try:
            if self.fila_ultima_cuota_pagada is not None:
                return parse_number(self.fila_ultima_cuota_pagada["cuota"])
            return None
        except Exception as e:
            logging.error("{} error: {}".format(DDATablesProcessor.get_ultima_cuota_pagada.__qualname__, e))
            self.errors.append("Última Cuota Pagada")
            return None

    def get_primera_cuota_impaga(self) -> Union[int, None]:
        try:
            if self.fila_primera_cuota_impaga is not None:
                return int(parse_number(self.fila_primera_cuota_impaga["cuota"]))
            return None
        except Exception as e:
            logging.error("{} error: {}".format(DDATablesProcessor.get_primera_cuota_impaga.__qualname__, e))
            self.errors.append("Cuota Impaga")
            return None

    def get_valor_cuota(self) -> Union[int, None]:
        try:
            valor_cuota = self.tabla_pagos["valor_cuota"].mode()[0]  # cuota inicial puede ser distinta al resto.
            # valor_cuota = self.tabla_pagos.loc[0, "valor_cuota"]
            return int(valor_cuota.replace(".", ""))
        except Exception as e:
            logging.error("{} error: {}".format(DDATablesProcessor.get_valor_cuota.__qualname__, e))
            if "Valor Cuota" not in self.errors:
                self.errors.append("Valor Cuota")
            return None

    def get_total_pagare(self) -> Union[int, None]:
        try:
            monto_actual = self.tabla_pagos.loc[0, "monto_actual"]
            return int(monto_actual.replace(".", ""))
        except Exception as e:
            logging.error("{} error: {}".format(DDATablesProcessor.get_total_pagare.__qualname__, e))
            self.errors.append("Total Pagaré")
            return None

    def get_monto_impago(self) -> Union[int, None]:
        try:
            if self.fila_ultima_cuota_pagada is not None:
                return parse_number(self.fila_ultima_cuota_pagada["saldo_capital"].replace(".", ""))

            else:
                return parse_number(self.fila_primera_cuota_impaga["monto_actual"])

        except Exception as e:
            logging.error("{} error: {}".format(DDATablesProcessor.get_monto_impago.__qualname__, e))
            self.errors.append("Monto Impago")
            return None

    def get_fecha_pago_primera_cuota(self) -> Union[datetime.datetime, None]:
        try:
            return parse_date(self.tabla_pagos.loc[0, "fvcto"], fuzzy=True, dayfirst=True)
        except Exception as e:
            logging.error("{} error: {}".format(DDATablesProcessor.get_fecha_pago_primera_cuota.__qualname__, e))
            self.errors.append("Fecha Pago Primera Cuota")
            return None

    def get_dia_pago(self) -> Union[int, None]:
        try:
            fecha = self.get_fecha_pago_primera_cuota()
            if fecha is not None:
                return fecha.day
            return None
        except Exception as e:
            logging.error("{} error: {}".format(DDATablesProcessor.get_dia_pago.__qualname__, e))
            self.errors.append("Día Pago")
            return None

    def get_fecha_mora(self) -> Union[datetime.datetime, None]:
        try:
            return parse_date(self.fila_primera_cuota_impaga["fvcto"], fuzzy=True, dayfirst=True)

        except Exception as e:
            logging.error("{} error: {}".format(DDATablesProcessor.get_fecha_mora.__qualname__, e))
            self.errors.append("Fecha Mora")
            return None

    def get_valor_ultima_cuota(self) -> Union[int, None]:
        try:
            if self.fila_cuoton is not None:
                cuota = self.get_valor_cuota()
                # print(self.fila_cuoton["valor_cuota"].item())
                ultima_cuota = parse_number(self.fila_cuoton["valor_cuota"].item())
                if ultima_cuota > cuota:
                    return ultima_cuota
            return None
        except Exception as e:
            logging.error("{} error: {}".format(DDATablesProcessor.get_valor_ultima_cuota.__qualname__, e))
            self.errors.append("Valor Ultima Cuota")
            return None


class DDATextProcessor:
    def __init__(self, text) -> None:
        self.text = text.lower()
        self.errors: List[str] = []

    def get_nombre(self) -> Union[str, None]:
        try:
            rut_pattern = re.compile(r"(?<=nombre)(.+?)(?=estado)")
            found = rut_pattern.findall(self.text)

            return found[0].replace(":", "").strip().title()

        except Exception as e:
            logging.error("{} error: {}".format(DDATextProcessor.get_nombre.__qualname__, e))
            return None

    def get_rut(self) -> Union[str, None]:
        try:
            rut_pattern = re.compile(r"(?<=r.u.t.)(.+?)(?=fecha)")
            found = rut_pattern.findall(self.text)
            # return found[0].split('-').replace(':', '').replace(' ', '').strip().upper()
            return parse_rut(found[0])

        except Exception as e:
            logging.error("{} error: {}".format(DDATextProcessor.get_rut.__qualname__, e))
            self.errors.append("RUT")
            return None

    def get_vendedor(self) -> Union[str, None]:
        try:
            pattern = re.compile(r"(?<=contrato:)(.+?)(?=nombre)")
            found = pattern.findall(self.text)

            if len(found) == 0:
                pattern_2 = re.compile(r"(?<=contrato\s:)(.+?)(?=nombre)")
                found = pattern_2.findall(self.text)

            return found[0].replace(":", "").strip().upper()
        except Exception as e:
            logging.error("{} error: {}".format(DDATextProcessor.get_vendedor.__qualname__, e))
            self.errors.append("Vendedor")
            return None

    def get_cuotas(self) -> Union[str, None]:
        try:
            rut_pattern = re.compile(r"plazo.+cuota:")
            found = rut_pattern.findall(self.text)

            return (
                found[0]
                .replace("plazo", "")
                .replace("(meses)", "")
                .replace("t.cuota", "")
                .replace(":", "")
                .strip()
                .title()
            )

        except Exception as e:
            logging.error("{} error: {}".format(DDATextProcessor.get_cuotas.__qualname__, e))
            self.errors.append("Número Cuotas")
            return None

    def get_tasa_interes(self) -> Union[float, None]:
        try:
            rut_pattern = re.compile(r"pagaré: \d{1,2},\d{1,2}\b")
            found = rut_pattern.findall(self.text)
            value = found[0].replace("pagaré", "").replace(":", "").replace(",", ".").strip()
            return round(float(value) / 100, 6)

        except Exception as e:
            logging.error("{} error: {}".format(DDATextProcessor.get_tasa_interes.__qualname__, e))
            self.errors.append("Tasa Interés")
            return None


class DesarrolloContratosProcessor(Processor):
    def process_response(self, response: Dict[str, Any], parsed_key: Dict[str, str]) -> pd.DataFrame:

        tables = [pd.DataFrame(table) for table in response["tables"]]
        text = response["text"]

        self.processed_tables = DDATablesProcessor(tables)
        self.processed_text = DDATextProcessor(text)

        row = {
            "RUT": self.processed_text.get_rut(),
            "Vendedor": self.processed_text.get_vendedor(),
            # 'Nº Cuotas': self.processed_forms.get_cuotas(),
            "Última Cuota Pagada": self.processed_tables.get_ultima_cuota_pagada(),
            "Cuota Impaga": self.processed_tables.get_primera_cuota_impaga(),
            # 'Tasa de Interes': self.processed_text.get_tasa_interes(),
            # "Valor Cuota": self.processed_tables.get_valor_cuota(),
            # "Total Pagaré": self.processed_tables.get_total_pagare(),
            "Monto Impago": self.processed_tables.get_monto_impago(),
            # "Fecha Pago Primera Cuota": self.processed_tables.get_fecha_pago_primera_cuota(),
            # "Día de pago": self.processed_tables.get_dia_pago(),
            "Fecha Mora": self.processed_tables.get_fecha_mora(),
            # "Valor Última Cuota": self.processed_tables.get_valor_ultima_cuota(),
            "Nombre Documento": parsed_key["filename_full"],
        }

        errores = self.processed_tables.errors + self.processed_text.errors
        row["Errores"] = ", ".join(errores)
        return pd.DataFrame([row])
