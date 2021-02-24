from copy import deepcopy
import json
from typing import List
from lambda_function import lambda_handler

EVENT_EXAMPLE = {
    "Records": [
        {
            "messageId": "7b8b846b-c337-402e-a7e9-039b26732ad9",
            "receiptHandle": "AQEBQF7b4JhT/Za6Y6L88tvrMGs7BQ1kg8P80OF5BG2tYzXlc97FtajS5MqPGodKAe90xBDE/9eu/wiK5YxzYzPShlB6gsqv1QJuNHWYeFMBAJbI0aGjDTNWmgWS/m0A1zwOvYHKn4m9UtXQP5hxyLbkKE5eV/je5F8M/5sfDP59Z0fT0/0wBwOiF0c3k3J7y5gezNl3QOGVHun19kM655D10MLG88gLxorWPg8ivfR8ZTzECjKsSAK4YlTft1ZRONdaV04LXCUb41ziCfAQZvybRRnjvf4k90CBaVBRg2o6guKGeGwMQ4dg+HGZAK9ybtyvXM+ZK8E+YoeKVRS8NeMjrbXq8/Hsizy8+F5mDsNCLsAm/n5kvqciV0dGSdNj7drRKdLVzBa123f9u7S6VZxD9Q==",
            "body": '{"version":"1.0","timestamp":"2021-01-22T21:07:59.688Z","requestContext":{"requestId":"aa3a590c-0f90-4948-9a34-22d48661af7f","functionArn":"arn:aws:lambda:us-east-2:454177333545:function:Mike_OCR_Map:$LATEST","condition":"Success","approximateInvokeCount":1},"requestPayload":{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-east-2","eventTime":"2021-01-22T21:07:37.096Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AWS:AIDAWTPYUEEU4Q7UDH44Z"},"requestParameters":{"sourceIPAddress":"18.221.137.189"},"responseElements":{"x-amz-request-id":"6E8F0BE117300E67","x-amz-id-2":"hIUXPOY40LHiFOb/DpOGdiC8BA1CIv71ywjPDWv3S1MP50+BuYs3q7TCyNtQBA49N8o5yzmmdTT4zACLf4DptE2r8HNVj5cj"},"s3":{"s3SchemaVersion":"1.0","configurationId":"de2b518a-0ddb-44e7-8025-9bb15c3b0723","bucket":{"name":"textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006","ownerIdentity":{"principalId":"AFTBJ9N16F5MT"},"arn":"arn:aws:s3:::textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006"},"object":{"key":"ExtractorDeudas/Entrada/Forum/2021-01-22T21_07_43_698499/muestreo_3.zip","size":3432036,"eTag":"f07a22f74e9355bebd479c6425824317","sequencer":"00600B3E9FBEE6005A"}}}]},"responseContext":{"statusCode":200,"executedVersion":"$LATEST"},"responsePayload":{"processable_files": ["ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/ALEJANDRO_TORRES_FUENTES_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/ANA_QUIROGA_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/KEVIN_ACEVEDO_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/MARGARITA_GOMEZ_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/ALEJANDRA_MEDINA_SUAREZ_-_PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/ALEJANDRO_TORRES_FUENTES_-_PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/JAVIERA_CONTRERAS_-_PAGAR.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/NIBALDO_PEREIRA_VALENZUELA_-_PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/NIBALDO_PEREIRA_VALENZUELA_-_TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/NIDIA_ARAYA_PASTENE_-_TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/PABLO_NILSON_GONZALEZ_-_TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/RENATO_RODRIGUEZ_-_TABLA.pdf"], "unprocessable_files": [], "empresa": "Forum", "folder_datetime": "2021-01-22T21_07_43_698499", "email": "pbadilla.torrealba@gmail.com", "number_of_retries": 0}}',
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1611349679725",
                "SenderId": "AROAWTPYUEEUQM3LJFI6F:awslambda_475_20210122210759691",
                "ApproximateFirstReceiveTimestamp": "1611350579725",
            },
            "messageAttributes": {},
            "md5OfBody": "709b0dc28a8df2c0a9e4222201842037",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-east-2:454177333545:Mike_OCR_Reduce_Queue",
            "awsRegion": "us-east-2",
        }
    ]
}


EVENT_TEMPLATE = {
    "Records": [
        {
            "messageId": "7b8b846b-c337-402e-a7e9-039b26732ad9",
            "receiptHandle": "AQEBQF7b4JhT/Za6Y6L88tvrMGs7BQ1kg8P80OF5BG2tYzXlc97FtajS5MqPGodKAe90xBDE/9eu/wiK5YxzYzPShlB6gsqv1QJuNHWYeFMBAJbI0aGjDTNWmgWS/m0A1zwOvYHKn4m9UtXQP5hxyLbkKE5eV/je5F8M/5sfDP59Z0fT0/0wBwOiF0c3k3J7y5gezNl3QOGVHun19kM655D10MLG88gLxorWPg8ivfR8ZTzECjKsSAK4YlTft1ZRONdaV04LXCUb41ziCfAQZvybRRnjvf4k90CBaVBRg2o6guKGeGwMQ4dg+HGZAK9ybtyvXM+ZK8E+YoeKVRS8NeMjrbXq8/Hsizy8+F5mDsNCLsAm/n5kvqciV0dGSdNj7drRKdLVzBa123f9u7S6VZxD9Q==",
            "body": '{"version":"1.0","timestamp":"2021-01-22T21:07:59.688Z","requestContext":{"requestId":"aa3a590c-0f90-4948-9a34-22d48661af7f","functionArn":"arn:aws:lambda:us-east-2:454177333545:function:Mike_OCR_Map:$LATEST","condition":"Success","approximateInvokeCount":1},"requestPayload":{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-east-2","eventTime":"2021-01-22T21:07:37.096Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AWS:AIDAWTPYUEEU4Q7UDH44Z"},"requestParameters":{"sourceIPAddress":"18.221.137.189"},"responseElements":{"x-amz-request-id":"6E8F0BE117300E67","x-amz-id-2":"hIUXPOY40LHiFOb/DpOGdiC8BA1CIv71ywjPDWv3S1MP50+BuYs3q7TCyNtQBA49N8o5yzmmdTT4zACLf4DptE2r8HNVj5cj"},"s3":{"s3SchemaVersion":"1.0","configurationId":"de2b518a-0ddb-44e7-8025-9bb15c3b0723","bucket":{"name":"textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006","ownerIdentity":{"principalId":"AFTBJ9N16F5MT"},"arn":"arn:aws:s3:::textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006"},"object":{"key":"ExtractorDeudas/Entrada/Forum/2021-01-22T21_07_43_698499/muestreo_3.zip","size":3432036,"eTag":"f07a22f74e9355bebd479c6425824317","sequencer":"00600B3E9FBEE6005A"}}}]},"responseContext":{"statusCode":200,"executedVersion":"$LATEST"},"responsePayload":',
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1611349679725",
                "SenderId": "AROAWTPYUEEUQM3LJFI6F:awslambda_475_20210122210759691",
                "ApproximateFirstReceiveTimestamp": "1611350579725",
            },
            "messageAttributes": {},
            "md5OfBody": "709b0dc28a8df2c0a9e4222201842037",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-east-2:454177333545:Mike_OCR_Reduce_Queue",
            "awsRegion": "us-east-2",
        }
    ]
}

EVENT_EXAMPLE_FROM_SQS = {
    "Records": [
        {
            "messageId": "f74bc9eb-e010-490a-b290-e2ec607e3ec5",
            "receiptHandle": "AQEB0OCXhwAFJRyaxOl9EF8FtQGz0ucAZSe9Ed+KQGstDNfGbj+KoE2nwGHciJLgTl43BrANuJzWvw87Tg2MeCH3QVcluhk/FCQ5P8CD0uxjtcjBCWAciG/hwm7DT1hYy9snI6QANn1cZVAa1UYTYW83TsqQtCpdSdrR48YVys5CI6BoVNEMQ/TYrbTtTNYkmyVaiEspDOtRaBLqIu+DHNS3jUO30OSgX4N1AHqFhWCVr4eHtE3xVjez55xdcvSrXCHPU74t2xnbZ12xe4RkJfPsuithBW+gOluEJRPTxLYvOcH4zyge+Ww8oFmi4Gh+ob743if0iT7UGzkuJoGDKpe5EAUeDlGeWbUXIV4jliPvuO9FAwmYIK66EdyblJ8X8yrfw+VXRb3Os4Qv2Ulzwv92Ow==",
            "body": '{"success": true, "processable_files": ["ExtractorDeudas/Archivo/Forum/2021-01-25T15_05_39_027637/ALEJANDRO_TORRES_FUENTES_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-25T15_05_39_027637/ANA_QUIROGA_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-25T15_05_39_027637/KEVIN_ACEVEDO_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-25T15_05_39_027637/MARGARITA_GOMEZ_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-25T15_05_39_027637/ALEJANDRA_MEDINA_SUAREZ_-_PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-25T15_05_39_027637/ALEJANDRO_TORRES_FUENTES_-_PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-25T15_05_39_027637/JAVIERA_CONTRERAS_-_PAGAR.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-25T15_05_39_027637/NIBALDO_PEREIRA_VALENZUELA_-_PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-25T15_05_39_027637/NIBALDO_PEREIRA_VALENZUELA_-_TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-25T15_05_39_027637/NIDIA_ARAYA_PASTENE_-_TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-25T15_05_39_027637/PABLO_NILSON_GONZALEZ_-_TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-01-25T15_05_39_027637/RENATO_RODRIGUEZ_-_TABLA.pdf"], "unprocessable_files": [], "empresa": "Forum", "folder_datetime": "2021-01-25T15_05_39_027637", "email": "pbadilla.torrealba@gmail.com", "number_of_retries": 1}',
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1611587280284",
                "SenderId": "AROAWTPYUEEU7CBDQZVQK:Mike_OCR_Reduce",
                "ApproximateFirstReceiveTimestamp": "1611587400284",
            },
            "messageAttributes": {},
            "md5OfBody": "40bf0e2a5c7f57cbbb82046c26271e4b",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-east-2:454177333545:Mike_OCR_Reduce_Queue",
            "awsRegion": "us-east-2",
        }
    ]
}


EVENT_EXAMPLE = {
    "Records": [
        {
            "messageId": "e1490684-6b22-493e-8766-736ffb5f346d",
            "receiptHandle": "AQEB0n03vAUee9BaEplXxETn8/6g3/aH5pSmMwyEgDNVL5UEjgRuk916VB4zeZIq5FlOPXkAmwLw+vtrMvjzokkUhoauazfwgY0hQDVeBxtLJu/vsxGWkSrIUDMt+mLu625vNZmp+qsMLUxhFWDJIuuZgEZp3lfI+AEsf4jlFSepq2Zgv4t5H9jyi7ytmC78whHYiUTsXSCCVKsYbtdCtWTset0+LJVHbuMZOVyY+q8NjmBAIK9oGaM9lv6F0/82pxOKxYJO2cZStLMH0SxnFy8duhCp5Cm+6wlXBZiQ8y8jyT4sBby+PEKlexf10RZMhUqk/KOzijaH7q1IkAQ+aYBqxC0vhby45DqroLet0dJIZkfUCwdGCOIRFaCV23Rwm019KXSarcKaVe3XFhOFlJ5/FA==",
            "body": '{"version":"1.0","timestamp":"2021-02-23T19:09:18.222Z","requestContext":{"requestId":"10a8381a-ef49-4ea5-be3d-8ef063d911f6","functionArn":"arn:aws:lambda:us-east-2:454177333545:function:Mike_OCR_Map:$LATEST","condition":"Success","approximateInvokeCount":1},"requestPayload":{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-east-2","eventTime":"2021-02-23T19:08:24.330Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AWS:AIDAWTPYUEEU4Q7UDH44Z"},"requestParameters":{"sourceIPAddress":"18.220.213.1"},"responseElements":{"x-amz-request-id":"PPF9FFHDYTJTRRDS","x-amz-id-2":"jml7XSfOFcYp6K7HlsFRVvx21ZM9TNJGKXflwOQgossjU5cmLK5Y82/xlHIftAMHdJpXXpJA+Sntf5EsBRUKr9pb+rNvg4qicVJLQ2Odkso="},"s3":{"s3SchemaVersion":"1.0","configurationId":"de2b518a-0ddb-44e7-8025-9bb15c3b0723","bucket":{"name":"textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006","ownerIdentity":{"principalId":"AFTBJ9N16F5MT"},"arn":"arn:aws:s3:::textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006"},"object":{"key":"ExtractorDeudas/Entrada/Forum/2021-02-23T19_08_27_740174/23-02.zip","size":24677042,"eTag":"d0d065a8ef4c78d0877e5dde337724db","sequencer":"00603552ABD12E3295"}}}]},"responseContext":{"statusCode":200,"executedVersion":"$LATEST"},"responsePayload":{"success": true, "processable_files": ["ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/DANNY_GUTIERREZ_ANDRADE_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/DANNY_GUTIERREZ_ANDRADE_-_PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/DANNY_GUTIERREZ_ANDRADE_-_TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/GUILLERMO_CACERES_LALUT_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/GUILLERMO_CACERES_LALUT_-_PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/GUILLERMO_CACERES_LALUT_-_TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/HUGO_PAVEZ_LOPEZ_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/HUGO_PAVEZ_LOPEZ_-_PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/HUGO_PAVEZ_LOPEZ_-_TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/ISABEL_DIAZ_FUENTEALBA_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/ISABEL_DIAZ_FUENTEALBA_-_PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/ISABEL_DIAZ_FUENTEALBA_-_TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/ISMAEL_YEVENES_LOPEZ_-_PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/ISMAEL_YEVENES_LOPEZ_-_TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/JOSE_SILVA_CERDA_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/JOSE_SILVA_CERDA_-_PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/JOSE_SILVA_CERDA_-_TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/JOSE_VALENCIA_BUSTOS_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/JOSE_VALENCIA_BUSTOS_-_PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/JOSE_VALENCIA_BUSTOS_-_TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/MARCO_RIOS_CONTRERAS_-_PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/MARCO_RIOS_CONTRERAS_-_TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/MARIA_GODOY_PERALTA_-_PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/MARIA_GODOY_PERALTA_-_TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/MARIA_VALENZUELA_CISTERNA_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/MARIA_VILLARROEL_TORO_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/MARIA_VILLARROEL_TORO_-_PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/MARIA_VILLARROEL_TORO_-_TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/PABLO_ERVIA_ILLANES_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/PABLO_ERVIA_ILLANES_-_PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/PABLO_ERVIA_ILLANES_-_TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/PABLO_MUNOZ_GODOY_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/SERV_DE_LIMP_Y_SANITIZACION_RAFAEL_BARRIA_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/SERVICIOS_DE_PUBLICIDAD_LIMPIEZAS_Y_SANITIZACION_-_TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/SERVICIOS_DE_PUBLICIDAD_LIMPIEZAS_Y_SANITIZACION_RAFAEL_RAMON_BARRIA_CHEUQUELAF_EIRL_-_PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/SOCIEDAD_COMERCIAL_RIOS_LIMITADA_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/TERESA_ESCOBAR_MACHUCA_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/TERESA_ESCOBAR_MACHUCA_-_PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/TERESA_ESCOBAR_MACHUCA_-_TABLA.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/VICTOR_CASTILLO_ORTEGA_-_CAV.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/VICTOR_CASTILLO_ORTEGA_-_PAGARE.pdf", "ExtractorDeudas/Archivo/Forum/2021-02-23T19_08_27_740174/VICTOR_CASTILLO_ORTEGA_-_TABLA.pdf"], "unprocessable_files": [], "empresa": "Forum", "folder_datetime": "2021-02-23T19_08_27_740174", "email": "", "number_of_retries": 0}}',
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1614107358261",
                "SenderId": "AROAWTPYUEEUQM3LJFI6F:awslambda_869_20210223190918227",
                "ApproximateFirstReceiveTimestamp": "1614107478261",
            },
            "messageAttributes": {},
            "md5OfBody": "5953802854ce9602ff8575b53e2d8077",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-east-2:454177333545:Mike_OCR_Reduce_Queue",
            "awsRegion": "us-east-2",
        }
    ]
}


def generate_event(
    processable_files: List[str],
    unprocessable_files: List[str],
    empresa: str,
    folder_datetime: str,
    email: str,
    number_of_retries: int,
):
    responsePayload = {
        "processable_files": processable_files,
        "unprocessable_files": unprocessable_files,
        "empresa": empresa,
        "folder_datetime": folder_datetime,
        "email": email,
        "number_of_retries": number_of_retries,
    }

    responsePayload_json = json.dumps(responsePayload)

    generated_event = deepcopy(EVENT_TEMPLATE)
    generated_event["Records"][0]["body"] += responsePayload_json
    generated_event["Records"][0]["body"] += "}"
    return generated_event


def test_generate_event():
    processable_files = [
        "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/ALEJANDRO_TORRES_FUENTES_-_CAV.pdf",
        "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/ANA_QUIROGA_-_CAV.pdf",
        "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/KEVIN_ACEVEDO_-_CAV.pdf",
        "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/MARGARITA_GOMEZ_-_CAV.pdf",
        "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/ALEJANDRA_MEDINA_SUAREZ_-_PAGARE.pdf",
        "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/ALEJANDRO_TORRES_FUENTES_-_PAGARE.pdf",
        "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/JAVIERA_CONTRERAS_-_PAGAR.pdf",
        "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/NIBALDO_PEREIRA_VALENZUELA_-_PAGARE.pdf",
        "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/NIBALDO_PEREIRA_VALENZUELA_-_TABLA.pdf",
        "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/NIDIA_ARAYA_PASTENE_-_TABLA.pdf",
        "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/PABLO_NILSON_GONZALEZ_-_TABLA.pdf",
        "ExtractorDeudas/Archivo/Forum/2021-01-22T21_07_43_698499/RENATO_RODRIGUEZ_-_TABLA.pdf",
    ]
    unprocessable_files = []
    empresa = "Forum"
    folder_datetime = "2021-01-22T21_07_43_698499"
    email = "pbadilla.torrealba@gmail.com"
    number_of_retries = 0
    event = generate_event(processable_files, unprocessable_files, empresa, folder_datetime, email, number_of_retries)

    assert EVENT_EXAMPLE == event


def test_reduce_function():

    processable_files = (
        [
            "ExtractorDeudas/Archivo/Forum/2021-01-28T17_15_49_143903/ANTHONY_VALLEJOS_VALENCIA_-_CAV.pdf",
            "ExtractorDeudas/Archivo/Forum/2021-01-28T17_15_49_143903/ANTHONY_VALLEJOS_VALENCIA_-_PAGARE.pdf",
            "ExtractorDeudas/Archivo/Forum/2021-01-28T17_15_49_143903/ANTHONY_VALLEJOS_VALENCIA_-_TABLA.pdf",
            "ExtractorDeudas/Archivo/Forum/2021-01-28T17_15_49_143903/JORGE_DIAZ_-_CAV.pdf",
            "ExtractorDeudas/Archivo/Forum/2021-01-28T17_15_49_143903/JORGE_DIAZ_-_PAGAR.pdf",
            "ExtractorDeudas/Archivo/Forum/2021-01-28T17_15_49_143903/JORGE_DIAZ_-_TABLA.pdf",
            "ExtractorDeudas/Archivo/Forum/2021-01-28T17_15_49_143903/JORGE_LUIS_ROJAS_-_CAV.pdf",
            "ExtractorDeudas/Archivo/Forum/2021-01-28T17_15_49_143903/JORGE_LUIS_ROJAS_-_PAGARE.pdf",
            "ExtractorDeudas/Archivo/Forum/2021-01-28T17_15_49_143903/JORGE_LUIS_ROJAS_-_TABLA.pdf",
            "ExtractorDeudas/Archivo/Forum/2021-01-28T17_15_49_143903/PABLO_NILSON_GONZALEZ_-_CAV.pdf",
            "ExtractorDeudas/Archivo/Forum/2021-01-28T17_15_49_143903/PABLO_NILSON_GONZALEZ_-_PAGARE.pdf",
            "ExtractorDeudas/Archivo/Forum/2021-01-28T17_15_49_143903/PABLO_NILSON_GONZALEZ_-_TABLA.pdf",
            "ExtractorDeudas/Archivo/Forum/2021-01-28T17_15_49_143903/SOLEDAD_ROJAS_COLLAO_-_CAV.pdf",
            "ExtractorDeudas/Archivo/Forum/2021-01-28T17_15_49_143903/SOLEDAD_ROJAS_COLLAO_-_PAGARE.pdf",
            "ExtractorDeudas/Archivo/Forum/2021-01-28T17_15_49_143903/SOLEDAD_ROJAS_COLLAO_-_TABLA.pdf",
        ],
    )

    unprocessable_files = []
    empresa = "Forum"
    folder_datetime = "2021-01-28T17_15_49_143903"
    email = "pbadilla.torrealba@gmail.com"
    number_of_retries = 100

    event = generate_event(processable_files, unprocessable_files, empresa, folder_datetime, email, number_of_retries)

    expected_output = {
        "email_sent": True,
        "enqueued_execution": False,
        "results_file": "https://textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006.s3.us-east-2.amazonaws.com/ExtractorDeudas/Salida/Forum/2021-01-25T12_45_44_250555/Resultados_OCR_2021-01-25T12_45_44_250555.xlsx",
        "success": True,
    }
    result = lambda_handler(EVENT_EXAMPLE, "")
    assert result == expected_output


if __name__ == "__main__":
    # test_generate_event()
    test_reduce_function()
    print("\033[92mAll test OK\033[0m")
