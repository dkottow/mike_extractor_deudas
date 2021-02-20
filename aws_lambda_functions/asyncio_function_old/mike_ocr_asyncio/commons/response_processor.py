import numpy as np
import pandas as pd
from .utils import estandarizar_palabra
from .response_parser import ResponseParser


class ResponseProcessor():
    @staticmethod
    def get_text(doc: ResponseParser):
        texto = ''
        for page in doc.pages:
            for line in page.lines:
                for word in line.words:
                    texto += word.text
                    texto += ' '
        return texto

    @staticmethod
    def get_forms(doc: ResponseParser):
        form = {}
        for page in doc.pages:
            for field in page.form.fields:
                field_name = estandarizar_palabra(field.key.text)
                try:
                    form[field_name] = field.value.text
                except Exception as e:
                    form[field_name] = None

        return form

    @staticmethod
    def get_tables(doc: ResponseParser):
        tablas = []
        for page in doc.pages:

            for table in page.tables:
                num_rows = len(table.rows)
                num_cols = len(table.rows[0].cells)

                tabla_generada = [[np.NAN] * num_cols for i in range(num_rows)]

                for r, row in enumerate(table.rows):
                    for c, cell in enumerate(row.cells):

                        tabla_generada[r][c] = cell.text

                tablas.append(pd.DataFrame(tabla_generada))

        return tablas
