class Config:
    S3_BUCKET = "textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006"
    REGION_NAME = "us-east-2"
    ROLE_ARN = "arn:aws:iam::454177333545:role/TextractRole"
    PATH_TO_MODEL = "./mike_ocr/forum/forum_clf_model"

    ENTRADA_PATH = "ExtractorDeudas/Entrada/Forum"
    ARCHIVO_PATH = "ExtractorDeudas/Archivo/Forum/"
    SALIDA_PATH = "ExtractorDeudas/Salida/Forum/"
    ERRORES_PATH = "ExtractorDeudas/Errores/Forum/"

    # Tiempo que esparará el await del textextract en segundos
    SECONDS_TO_AWAIT_FOR_TEXTEXTRACT_RESPONSE = 20

    FORUM_DOCUMENT_TYPES = ["cav", "pagare", "tabla"]

    COL_ORDER = [
        "Nombre y Apellido",
        "RUT",
        "Representante",
        "Rut Representante",
        "Dirección",
        "Comuna",
        "Nº Documento",
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
        "Tipo de Vehículo",
        "Marca",
        "Modelo",
        "Nº Motor",
        "Nº Chassis",
        "Color",
        "Año",
        "Nº Inscripción",
        "Archivo Pagare",
        "Errores Pagare",
        "Archivo CAV",
        "Errores CAV",
        "Archivo Desarrollo Contratos",
        "Errores Desarrollo Contratos",
    ]

    COLS_REQUIRED_BY_DOC_TYPES = {
        "cav": [
            "RUT",
            "Tipo de Vehículo",
            "Marca",
            "Modelo",
            "Nº Motor",
            "Nº Chassis",
            "Color",
            "Año",
            "Nº Inscripción",
            "Archivo CAV",
            "Errores CAV",
        ],
        "pagare": [
            "Nombre y Apellido",
            "RUT",
            "Representante",
            "Rut Representante",
            "Dirección",
            "Comuna",
            "Nº Documento",
            "Nº Cuotas",
            "Tasa de Interes",
            "Archivo Pagare",
            "Errores Pagare",
        ],
        "tabla": [
            "RUT",
            "Última Cuota Pagada",
            "Cuota Impaga",
            "Valor Cuota",
            "Total Pagaré",
            "Monto Impago",
            "Fecha Pago Primera Cuota",
            "Día de pago",
            "Fecha Mora",
            "Valor Última Cuota",
            "Archivo Desarrollo Contratos",
            "Errores Desarrollo Contratos",
        ],
    }
