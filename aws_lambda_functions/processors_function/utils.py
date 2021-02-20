import os
from typing import Dict


def parse_mike_key(key: str) -> Dict[str, str]:
    # Conveción: ExtractorDeudas/{Bandeja}/{Empresa}/{DateTime}/{NombreArchivo}.{Extensión}
    # Bandeja = {Entrada,Salida,Archivo,Errores}
    try:
        splitted_key = key.split("/")
        splitted_filename = splitted_key[-1].split(".")

        parsed_key = {
            "bandeja": splitted_key[1],
            "empresa": splitted_key[2],
            "datetime": splitted_key[3],
            "filename_full": splitted_key[4],
            "filename": ".".join(splitted_filename[0:-1]),
            "extension": splitted_filename[-1],
        }

        return parsed_key

    except Exception as e:
        msg = f"Error while trying to parse {key}: {e}"
        raise Exception(msg)


def parsed_mike_key_to_str(parsed_key: Dict[str, str], new_bandeja: str = None, new_extension: str = None) -> str:

    if new_bandeja is not None:
        bandeja = new_bandeja
    else:
        bandeja = parsed_key["bandeja"]

    if new_extension is not None:
        extension = new_extension
    else:
        extension = parsed_key["extension"]

    empresa = parsed_key["empresa"]
    datetime = parsed_key["datetime"]
    filename = parsed_key["filename"]

    return f"ExtractorDeudas/{bandeja}/{empresa}/{datetime}/{filename}.{extension}"


def get_filename(key: str) -> str:
    return key.split("/")[-1].split(".")[0]


def get_extension(key: str) -> str:
    return key.split("/")[-1].split(".")[1]


def generate_mike_key(bandeja: str, empresa: str, datetime: str, filename: str, extension: str):
    return f"ExtractorDeudas/{bandeja}/{empresa}/{datetime}/{filename}.{extension}"


def generate_local_key(bandeja: str, datetime: str, filename_with_extension: str) -> str:
    return os.path.join(bandeja, datetime, filename_with_extension).replace("\\", "/")


import re
from typing import Dict, List
import string

import pandas as pd
import numpy as np

from dateutil.parser import parse
from sklearn.feature_extraction.text import strip_accents_ascii

punct_trans = str.maketrans("", "", "!\"#$&'()*+,-./:;<=>?@[\\]^_`{|}~")


def standardize_word(palabra: str):
    palabra = palabra.translate(punct_trans)
    palabra = " ".join(palabra.split()).strip().lower()
    palabra = strip_accents_ascii(palabra)
    return palabra.replace(" ", "_")


def standardize_text(texto: str):
    return texto.lower()


def concatenar_fila(fila: pd.Series):
    fila = fila.astype(str).str.lower().str.normalize("NFKD").str.encode("ascii", errors="ignore").str.decode("utf-8")
    return " ".join(fila)


def is_date(string_to_parse: str, fuzzy: bool = True):
    """
    Return whether the string can be interpreted as a date."""

    try:
        parse(string_to_parse, fuzzy=fuzzy)
        return True

    except ValueError:
        return False


_pattern = r"""(?x)       # enable verbose mode (which ignores whitespace and comments)
    ^                     # start of the input
    [^\d+-\.]*            # prefixed junk
    (?P<number>           # capturing group for the whole number
        (?P<sign>[+-])?       # sign group (optional)
        (?P<integer_part>         # capturing group for the integer part
            \d{1,3}               # leading digits in an int with a thousands separator
            (?P<sep>              # capturing group for the thousands separator
                [ ,.]                 # the allowed separator characters
            )
            \d{3}                 # exactly three digits after the separator
            (?:                   # non-capturing group
                (?P=sep)              # the same separator again (a backreference)
                \d{3}                 # exactly three more digits
            )*                    # repeated 0 or more times
        |                     # or
            \d+                   # simple integer (just digits with no separator)
        )?                    # integer part is optional, to allow numbers like ".5"
        (?P<decimal_part>     # capturing group for the decimal part of the number
            (?P<point>            # capturing group for the decimal point
                (?(sep)               # conditional pattern, only tested if sep matched
                    (?!                   # a negative lookahead
                        (?P=sep)              # backreference to the separator
                    )
                )
                [.,]                  # the accepted decimal point characters
            )
            \d+                   # one or more digits after the decimal point
        )?                    # the whole decimal part is optional
    )
    [^\d]*                # suffixed junk
    $                     # end of the input
"""


def parse_number(text):
    match = re.match(_pattern, text)
    if match is None or not (match.group("integer_part") or match.group("decimal_part")):  # failed to match
        return None  # consider raising an exception instead

    num_str = match.group("number")  # get all of the number, without the junk
    sep = match.group("sep")
    if sep:
        num_str = num_str.replace(sep, "")  # remove thousands separators

    if match.group("decimal_part"):
        point = match.group("point")
        if point != ".":
            num_str = num_str.replace(point, ".")  # regularize the decimal point
        return float(num_str)

    return int(num_str)


from itertools import cycle


def calc_digito_verificador(rut):
    reversed_digits = map(int, reversed(str(rut)))
    factors = cycle(range(2, 8))
    s = sum(d * f for d, f in zip(reversed_digits, factors))
    return (-s) % 11


def parse_rut(rut):
    # Eliminar dígito verificador (se equivocaba harto en esto...)
    rut = rut.split("-")[0]

    # Eliminar cualquier tipo de puntuación
    rut = rut.translate(punct_trans)

    # Parsear numero
    rut = parse_number(rut)

    # Calcular dígito verificador
    digito_verificador = calc_digito_verificador(rut)

    rut_formateado = "{:,}".format(rut).replace(",", ".")

    return "{}-{}".format(rut_formateado, digito_verificador if digito_verificador != 10 else "K").replace(".", "")


def is_numeric(obj):
    attrs = ["__add__", "__sub__", "__mul__", "__truediv__", "__pow__"]
    return all(hasattr(obj, attr) for attr in attrs)


class bcolors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def compare_results(validation_data: pd.DataFrame, results: Dict, verbose: bool = True):
    results = results["data"].to_dict(orient="records")[0]

    row = validation_data.loc[validation_data["RUT"] == results["RUT"]]
    row = row.to_dict(orient="records")[0]

    correct = 0
    total = len(results.keys()) - 2  # Se le restan las columnas Archivo y Errores

    ok_keys = []
    err_keys = []
    not_found_keys: List[str] = []

    for results_col, results_value in results.items():

        # No considedar columnas Archivo y Errores
        if "Archivo" in results_col or "Errores" in results_col:
            continue

        # Obtener el valor de validación
        try:
            validation_value = row.get(results_col)

            def print_ok():
                if verbose:
                    print(
                        f'{bcolors.OKGREEN}[OK]: "{results_col}" = "{validation_value}" fue detectado correctamente.{bcolors.ENDC}'
                    )

            def print_err():
                if verbose:
                    print(
                        f'{bcolors.FAIL}[ERROR]: "{results_col}" difiere: "{validation_value}" en los datos de validación, "{results_value}" en los resultados.'
                    )

            try:
                # Caso de strings:
                if isinstance(validation_value, str) and isinstance(results_value, str):

                    # Estandarizar strings
                    if isinstance(validation_value, str):
                        validation_value = validation_value.title().strip()
                    if isinstance(results_value, str):
                        results_value = results_value.title().strip()

                    # Validar
                    if validation_value == results_value:
                        print_ok()
                        ok_keys.append(results_col)
                        correct += 1

                    else:
                        print_err()
                        err_keys.append(results_col)

                # Caso Nan y None
                elif pd.isna(validation_value):
                    # Estandarizar nan y None:
                    validation_value = None

                    # Validar
                    if validation_value == results_value:
                        print_ok()
                        ok_keys.append(results_col)
                        correct += 1
                    else:
                        print_err()
                        err_keys.append(results_col)

                # Caso de números, comparación aproximada
                elif is_numeric(validation_value) and is_numeric(results_value):

                    if np.isclose(results_value, validation_value):
                        print_ok()
                        ok_keys.append(results_col)
                        correct += 1
                    else:
                        print_err()
                        err_keys.append(results_col)

                # Cualquier otro caso:
                else:
                    if str(validation_value) == str(results_value):
                        print_ok()
                        ok_keys.append(results_col)
                        correct += 1
                    else:
                        print_err()
                        err_keys.append(results_col)
            except Exception as e:
                print(
                    f"{bcolors.WARNING}[EXCEPCIÓN]: Excepción en {results_col}\nValor de validación: {validation_value}, Valor Detectado: {results_value})\nDescripción de la excepción: {e}{bcolors.ENDC}"
                )
                err_keys.append(results_col)

        except Exception as e:
            print(e)

            print(
                f"{bcolors.WARNING}[NO EXISTE]: No existe la columna {results_col}"
                " en los datos de validación {bcolors.ENDC}"
            )

    precision = np.round(correct / total, decimals=4)
    print(f"\n{bcolors.BOLD}{bcolors.OKBLUE}[PRECISION]: {precision * 100}%{bcolors.ENDC}{bcolors.ENDC}")

    return {
        "precision": precision,
        "correctly_detected": ok_keys,
        "incorrectly_detected": err_keys,
        "not_found": not_found_keys,
    }
