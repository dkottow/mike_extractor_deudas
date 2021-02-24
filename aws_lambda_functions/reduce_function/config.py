class Config:
    S3_BUCKET = "textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006"
    MODEL_PATH = "./forum/forum.model"
    ROLE_ARN = "arn:aws:iam::454177333545:role/TextractRole"
    AWS_REGION = "us-east-2"

    ENTRADA_PATH = "ExtractorDeudas/Entrada"
    ARCHIVO_PATH = "ExtractorDeudas/Archivo"
    SALIDA_PATH = "ExtractorDeudas/Salida"
    ERRORES_PATH = "ExtractorDeudas/Errores"

    # Configuración de los emails.
    EMAIL_SENDER = "pablo.badilla@ug.uchile.cl"
    EMAIL_BODY_TEXT = "Link a los resultados del OCR: {}"
    EMAIL_SUBJECT = "OCR Forum"
    EMAIL_CHARSET = "UTF-8"

    # Configuración de la cola.

    # Número de veces que se invocará el reduce para verificar si están todos los resultados
    # antes de que omita los que no encontró.
    MAX_RETRY_ATTEMPTS = 7
    SQS_URL = "https://sqs.us-east-2.amazonaws.com/454177333545/Mike_OCR_Reduce_Queue"

    SUCCESS_EMAIL_BODY_HTML = """<html>
    <head></head>
    <body>
    <h1>Detección de documentos Forum</h1>
    <br/>
    <p>En el siguiente <a href='{}'>link</a> podrá encontrar los resultados de la detección solicitados.</p>
    <br/>
    <p>Saludos</p>
    </body>
    </html>"""

    ERROR_EMAIL_BODY_HTML = """<html>
    <head></head>
    <body>
    <h1>Detección de documentos Forum</h1>
    <br/>
    <p>Ha ocurrido un Error al intentar analizar los documentos solicitados.</p>
    <p>Si el error persiste, comunicarse con soporte.</p>
    <br/>
    <p>Saludos</p>
    </body>
    </html>"""

    DOCUMENT_TYPES = ["cav", "pagare", "tabla"]

    COMPLETE_AND_PARTIAL_COLS = [
        "Demanda",
        "Vendedor",
        "Celular",
        "Celular 2",
        "Mail",
        "Nombre y Apellido",
        "RUT",
        "Representante",
        "Rut Representante",
        "Dirección",
        "Comuna",
        "Tipo Documento",
        "Nº Documento",
        "fecha Suscrfipción",
        "Nº Cuotas",
        "Última Cuota Pagada",
        "Cuota Impaga",
        "Tasa de Interes",
        "Valor Cuota",
        "Total Pagaré",
        "Monto Impago",
        "Fecha Pago Primera Cuota",
        "Día de pago",
        "Fecha Mora",
        "Valor Última Cuota",
        "Nombre Abogado",
        "Rut Abogado",
        "Codeudor",
        "Rut Codeudor",
        "Dir Codeudor",
        "Comuna codeudor",
        "Codeudor2",
        "Rut Codeudor2",
        "Direción Codeudor 2",
        "Comuna Codeudor 2",
        "Codeudor3",
        "Rut Codeudor3",
        "Dir Codeudor 3",
        "Comuna Codeudor 3",
        "Tipo de Vehículo",
        "Marca",
        "Modelo",
        "Nº Motor",
        "Nº Chassis",
        "Color",
        "Año",
        "Nº Inscripción",
    ]

    PROCESSED_DOCS_COLS = ["Nombre Documento", "Estado Procesamiento", "Tipo de Error"]

    DEFAULT_VALUES = {
        "Nombre Abogado": "Francisco Sotta Benapres",
        "Rut Abogado": "8.775.438-3",
        "Tipo Documento": "Pagaré",
    }

    DETECTION_COLUMNS = {
        "cav": {
            "RUT": {"required": True, "output": True},
            "Tipo de Vehículo": {"required": True, "output": True},
            "Marca": {"required": True, "output": True},
            "Modelo": {"required": True, "output": True},
            "Nº Motor": {"required": True, "output": True},
            "Nº Chassis": {"required": True, "output": True},
            "Color": {"required": True, "output": True},
            "Año": {"required": True, "output": True},
            "Nº Inscripción": {"required": True, "output": True},
        },
        "pagare": {
            "Nombre y Apellido": {"required": True, "output": True},
            "RUT": {"required": True, "output": True},
            "Representante": {"required": False},
            "Rut Representante": {"required": False},
            "Dirección": {"required": True, "output": True},
            "Comuna": {"required": True, "output": True},
            "Nº Documento": {"required": True, "output": True},
            "Nº Cuotas": {"required": True, "output": True},
            "Tasa de Interes": {"required": True, "output": True},
            "fecha Suscrfipción": {"required": True, "output": True},
            "Día de pago": {"required": True, "output": True},
            "Fecha Pago Primera Cuota": {"required": True, "output": True},
            "Total Pagaré": {"required": True, "output": True},
            "Valor Cuota": {"required": True, "output": True},
            "Valor Última Cuota": {"required": False, "output": True},
            "Codeudor": {"required": False, "output": True},
            "Rut Codeudor": {"required": False, "output": True},
            "Dir Codeudor": {"required": False, "output": True},
            "Comuna codeudor": {"required": False, "output": True},
            "Codeudor2": {"required": False, "output": True},
            "Rut Codeudor2": {"required": False, "output": True},
            "Direción Codeudor 2": {"required": False, "output": True},
            "Comuna Codeudor 2": {"required": False, "output": True},
            "Codeudor3": {"required": False, "output": True},
            "Rut Codeudor3": {"required": False, "output": True},
            "Dir Codeudor 3": {"required": False, "output": True},
            "Comuna Codeudor 3": {"required": False, "output": True},
        },
        "tabla": {
            "Vendedor": {"required": True, "output": True},
            "RUT": {"required": True, "output": True},
            "Última Cuota Pagada": {"required": True, "output": True},
            "Cuota Impaga": {"required": True, "output": True},
            #            "Valor Cuota": {"required": True, "output": True},
            #            "Total Pagaré": {"required": True, "output": True},
            #            "Fecha Pago Primera Cuota": {"required": True, "output": True},
            #            "Día de pago": {"required": True, "output": True},
            #            "Valor Última Cuota": {"required": False, "output": True},
            "Monto Impago": {"required": True, "output": True},
            "Fecha Mora": {"required": True, "output": True},
        },
        "all": {
            "Nombre Documento": {"document_type": "all", "required": True, "output": False},
            "Errores": {"document_type": "all", "required": True, "output": False},
        },
    }
