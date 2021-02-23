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

    EVENT_EXAMPLE = {
        "Records": [
            {
                "messageId": "f6b48c4c-94af-455b-9454-296223990af8",
                "receiptHandle": "AQEByQwP9t9MUirb8cKsn0lXB3BWXoU1cOjzkV9sIv3xFq0575YR1YOBoFumfhbCge+u73I+8OCZXz4HwhskJCs3/O5nQamxfBCxdB14YJL7dgxy5zioTpwYtiGtb8vwibQXqLbSwzja0v9hjK32frdIrqPK8VMKxCJ9I7x7pQIRr5XiRO/lyD+IGTblEk77fTdOe1X7VNCp9siJtpvyGyNbEigbWAEyXaalyA8cq8BhthQ+kt9QopOb1AiyrVCJRoCmUiHW4Q8LU0PNfGRZOawP4WYQWQz59Jmrcksb5NFfNNZ1njqmKACn2ibIQMHiJFPbaJ14hDtRs1BTx5+wslqy1I5p4mRr+YAX5x4bFxKQtj6ddS3Uv54iE+PoyD/IOToE3NqEXzFKPhkOPRwUCQgm+w==",
                "body": '{"version":"1.0","timestamp":"2021-01-18T14:53:35.142Z","requestContext":{"requestId":"329b02ff-c085-4087-b2e8-c7b5a7146026","functionArn":"arn:aws:lambda:us-east-2:454177333545:function:Mike_OCR_Map:$LATEST","condition":"Success","approximateInvokeCount":1},"requestPayload":{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-east-2","eventTime":"2021-01-18T14:53:28.599Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AWS:AIDAWTPYUEEU4Q7UDH44Z"},"requestParameters":{"sourceIPAddress":"3.84.197.224"},"responseElements":{"x-amz-request-id":"621ECE3D295E2D88","x-amz-id-2":"n/5A0OhI+LGY0+KKvjlGohy/kIQpJkObxOPCSb/3V4HBCqBpHEiTJm2lqTIOmZUG2lLbn7zjpLaqiiKtfoPg43KUF6UOCCzM"},"s3":{"s3SchemaVersion":"1.0","configurationId":"de2b518a-0ddb-44e7-8025-9bb15c3b0723","bucket":{"name":"textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006","ownerIdentity":{"principalId":"AFTBJ9N16F5MT"},"arn":"arn:aws:s3:::textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006"},"object":{"key":"ExtractorDeudas/Entrada/Forum/2021-01-18T14_53_28_906624/muestreo_3.zip","size":3432036,"eTag":"f07a22f74e9355bebd479c6425824317","sequencer":"006005A0E8F2962E36"}}}]},"responseContext":{"statusCode":200,"executedVersion":"$LATEST"},"responsePayload":{"success": true, "processable_files": ["ExtractorDeudas/Archivo/Forum/2021-01-18T14_53_28_906624/ALEJANDRO TORRES FUENTES - CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-18T14_53_28_906624/ANA QUIROGA - CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-18T14_53_28_906624/KEVIN ACEVEDO - CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-18T14_53_28_906624/MARGARITA GOMEZ - CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-18T14_53_28_906624/ALEJANDRA MEDINA SUAREZ - PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-18T14_53_28_906624/ALEJANDRO TORRES FUENTES - PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-18T14_53_28_906624/JAVIERA CONTRERAS - PAGAR\\u2558.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-18T14_53_28_906624/NIBALDO PEREIRA VALENZUELA - PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-18T14_53_28_906624/NIBALDO PEREIRA VALENZUELA - TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-18T14_53_28_906624/NIDIA ARAYA PASTENE - TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-18T14_53_28_906624/PABLO NILSON GONZALEZ - TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-18T14_53_28_906624/RENATO RODRIGUEZ - TABLA.pdf"], "unprocessable_files": [], "empresa": "Forum", "folder_datetime": "2021-01-18T14_53_28_906624", "email": "pbadilla.torrealba@gmail.com"}}',
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1610981615180",
                    "SenderId": "AROAWTPYUEEUQM3LJFI6F:awslambda_236_20210118145335148",
                    "ApproximateFirstReceiveTimestamp": "1610981625180",
                },
                "messageAttributes": {},
                "md5OfBody": "5be4eac34997ebd969594b221a999a45",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-2:454177333545:Mike_OCR_Reduce_Queue",
                "awsRegion": "us-east-2",
            }
        ]
    }
