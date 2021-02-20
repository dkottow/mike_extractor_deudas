import os
from typing import Dict
import unicodedata
import string

valid_filename_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
char_limit = 255


def get_safe_filename(filename, whitelist=valid_filename_chars, replace=" "):
    # replace spaces
    for r in replace:
        filename = filename.replace(r, "_")

    # keep only valid ascii chars
    cleaned_filename = unicodedata.normalize("NFKD", filename).encode("ASCII", "ignore").decode()

    # keep only whitelisted chars
    cleaned_filename = "".join(c for c in cleaned_filename if c in whitelist)
    if len(cleaned_filename) > char_limit:
        print(
            "Warning, filename truncated because it was over {}. Filenames may no longer be unique".format(char_limit)
        )
    return cleaned_filename[:char_limit]


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
    return f"ExtractorDeudas/{bandeja}/{empresa}/{datetime}/{filename}.{extension}".replace(" ", "_")


def generate_local_key(bandeja: str, datetime: str, filename_with_extension: str) -> str:
    return os.path.join(bandeja, datetime, filename_with_extension).replace("\\", "/").replace(" ", "_")

