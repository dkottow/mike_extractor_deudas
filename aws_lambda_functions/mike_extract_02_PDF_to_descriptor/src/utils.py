import logging
import string
import unicodedata
from pathlib import Path
from typing import Any, Union

logger = logging.getLogger()
logger.setLevel(logging.INFO)

VALID_FILENAME_CHARS = "-_.() %s%s" % (string.ascii_letters, string.digits)
CHAR_LIMIT = 255
BANDEJAS = ["Archivo", "Entrada", "Intermedio", "Salida", "Errores"]


class MikeKey:
    """Clase encargada de manejar las conveciones de rutas de la aplicación."""

    def __init__(
        self,
        key: Union[str, Any, None] = None,
        bandeja: Union[str, None] = None,
        empresa: Union[str, None] = None,
        datetime: Union[str, None] = None,
        etapa_intermedio: Union[str, None] = None,
        filename: Union[str, None] = None,
        extension: Union[str, None] = None,
    ):
        """Instancia el objeto que manejara las convenciones de rutas. Si se especifica key, se ignoran los otros args.

        Conveción de las rutas: {Bandeja}/{Empresa}/{DateTime}/{NombreArchivo}.{Extensión}
        Posibles valores para la bandeja = {Archivo, Entrada ,Intermedio ,Salida ,Errores}

        En el caso de que se esté usando una subcarpeta en la bandeja de intermedio, la conveción queda:
        Conveción de las rutas: Intermedio/{Intermedio}/{Empresa}/{DateTime}/{NombreArchivo}.{Extensión}


        Args:
            key (Union[str, None], optional): Key completa. Si se especifica, se descartarán los demas argumentos.
            Defaults to None.
            bandeja (Union[str, None], optional): Bandeja. Defaults to None.
            empresa (Union[str, None], optional): Nombre de la empresa. Defaults to None.
            datetime (Union[str, None], optional): Datetime (o test...). Defaults to None.
            filename (Union[str, None], optional): Nombre del archivo. Defaults to None.
            extension (Union[str, None], optional): Extensión del archivo sin punto. Defaults to None.
            etapa_intermedio (Union[str, None], optional): Subcarpeta de la etapa de procesamiento ubicada en
            la carpeta de intermedio. Se ocupa solo en caso que Bandeja=Intermedio. Defaults to None.

        Raises:
            Exception: Si es que no puede generar la ruta.
        """

        saved_args = locals()
        # Constructor 1: Obtener las rutas desde la key:
        if key is not None:
            try:
                if hasattr(key, "key"):
                    self.key = key.key
                else:
                    self.key = Path(key)

                # Parte de las rutas. Ej: ruta/a/alla -> ('ruta', 'a', 'alla')
                parts = self.key.parts

                self.bandeja = parts[0]
                # Verificar si la bandeja está correcta:
                if self.bandeja not in BANDEJAS:
                    msg = f'Bandeja "{bandeja}" not in {BANDEJAS}.'
                    raise Exception(msg)

                # Manejar el caso en que nos encontremos con un intermedio
                if "Intermedio" in parts:
                    if len(parts) < 5:
                        raise Exception(
                            f'Bandeja "Intermedio" must have "etapa_intermedio" folder in the path. Given: {parts}'
                        )
                    self.etapa_intermedio = parts[1]
                    self.empresa = parts[2]
                    self.datetime = parts[3]
                    self.filename_full = parts[4]

                else:
                    self.empresa = parts[1]
                    self.datetime = parts[2]
                    self.filename_full = parts[3]
                    self.etapa_intermedio = None

                self.filename = self.key.stem
                self.extension = self.key.suffix

            except Exception as e:
                msg = f'Error while trying to parse the given key "{key}": {e}'
                raise Exception(msg)

        # Constructor 2: Obtener la ruta desde los argumentos ignorando la key.
        else:

            # type checking
            msg = "Error while trying to generate the key: {} is not a string. Arguments: {}:"
            if not isinstance(empresa, str):
                raise Exception(msg.format(empresa, saved_args))
            if not isinstance(bandeja, str):
                raise Exception(msg.format(bandeja, saved_args))
            if not isinstance(datetime, str):
                raise Exception(msg.format(datetime, saved_args))
            if not isinstance(filename, str):
                raise Exception(msg.format(filename, saved_args))
            if not isinstance(extension, str):
                raise Exception(msg.format(extension, saved_args))

            # Verificar si es una bandeja válida.
            if bandeja not in BANDEJAS:
                msg = f'Error while trying to parse the given arguments {saved_args}: Bandeja "{bandeja}" not in {BANDEJAS}.'
                raise Exception(msg)

            self.bandeja = bandeja
            self.empresa = empresa
            self.datetime = datetime
            self.filename = filename
            self.extension = f".{extension}" if "." not in extension else extension
            self.filename_full = f"{self.filename}{self.extension}"

            if bandeja == "Intermedio":
                if not isinstance(etapa_intermedio, str):
                    msg = f'Error while trying to parse the given arguments {saved_args}: etapa_intermedio "{etapa_intermedio}" not a str.'
                    raise Exception(msg)
                self.etapa_intermedio = etapa_intermedio
                self.key = Path(f"{bandeja}/{etapa_intermedio}/{empresa}/{datetime}/{filename}{extension}")

            else:
                self.etapa_intermedio = None
                self.key = Path(f"{bandeja}/{empresa}/{datetime}/{filename}{extension}")

    def to_str(
        self,
        new_bandeja: Union[str, None] = None,
        new_empresa: Union[str, None] = None,
        new_datetime: Union[str, None] = None,
        new_filename: Union[str, None] = None,
        new_extension: Union[str, None] = None,
        new_etapa_intermedio: Union[str, None] = None,
    ) -> str:
        """Contruye un string a partir de la key guardada en el objeto y de los argumentos provistos.
        Los argumentos son opcionales. Si se especifica un argumento, se creará la ruta usando estos valores, pero no
        se reemplazaran en el objeto original.

        Args:
            new_bandeja (Union[str, None], optional): Nuevo valor para bandeja. Defaults to None.
            new_empresa (Union[str, None], optional): Nuevo valor para empresa. Defaults to None.
            new_datetime (Union[str, None], optional): Nuevo valor para datetime. Defaults to None.
            new_filename (Union[str, None], optional): Nuevo valor para filename. Defaults to None.
            new_extension (Union[str, None], optional): Nuevo valor para extensión. Se le agrega un punto al inicio si
            no tiene. Defaults to None.

        Returns:
            str: La ruta generada en str.
        """

        bandeja = new_bandeja or self.bandeja
        empresa = new_empresa or self.empresa
        datetime = new_datetime or self.datetime
        filename = new_filename or self.filename
        extension = new_extension or self.extension
        # Agregar punto
        extension = f".{extension}" if "." not in extension else extension

        if bandeja == "Intermedio":
            etapa_intermedio = new_etapa_intermedio or self.etapa_intermedio
            return f"{bandeja}/{etapa_intermedio}/{empresa}/{datetime}/{filename}{extension}"

        return f"{bandeja}/{empresa}/{datetime}/{filename}{extension}"

    def __str__(self) -> str:
        return self.to_str()

    # def __repr__(self) -> str:
    #     return f"<{self.key}>"

    def get_empresa(self) -> str:
        return self.empresa

    def get_bandeja(self) -> str:
        return self.bandeja

    def get_datetime(self) -> str:
        return self.datetime

    def get_filename(self) -> str:
        return self.filename

    def get_extension(self) -> str:
        return self.extension

    def get_etapa_intermedio(self) -> str:
        return self.etapa_intermedio

    def get_filename_with_ext(self) -> str:
        return self.filename_full


def get_safe_filename(filename, whitelist=VALID_FILENAME_CHARS, replace=" "):
    # replace spaces
    for r in replace:
        filename = filename.replace(r, "_")

    # keep only valid ascii chars
    cleaned_filename = unicodedata.normalize("NFKD", filename).encode("ASCII", "ignore").decode()

    # keep only whitelisted chars
    cleaned_filename = "".join(c for c in cleaned_filename if c in whitelist)
    if len(cleaned_filename) > CHAR_LIMIT:
        print(
            "Warning, filename truncated because it was over {}. Filenames may no longer be unique".format(CHAR_LIMIT)
        )
    return cleaned_filename[:CHAR_LIMIT]


def get_filename(path: str) -> str:
    return Path(path).stem


def get_extension(path: str) -> str:
    return Path(path).suffix

