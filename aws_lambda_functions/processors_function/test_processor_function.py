from lambda_function import lambda_handler


def test_processor_with_cav():
    event = {"Records": [{"s3": {"object": {"key": "ExtractorDeudas/Archivo/Forum/Test/CAMILA_SEGUEL_-_CAV.json"}}}]}

    expected_results = {
        "document_type": "cav",
        "key": "ExtractorDeudas/Archivo/Forum/Test/CAMILA_SEGUEL_-_CAV.pdf",
        "data": '{"RUT":{"0":"17028624-3"},"Tipo de Veh\\u00edculo":{"0":"Automovil"},"Marca":{"0":"Audi"},"Modelo":{"0":"A1 Sportback Attraction 1.4 Aut"},"N\\u00ba Motor":{"0":"CAX D59884"},"N\\u00ba Chassis":{"0":"WAUZZZ8X7DB118623"},"Color":{"0":"Plateado Hielo"},"A\\u00f1o":{"0":"2013"},"N\\u00ba Inscripci\\u00f3n":{"0":"GPGX.10-8"},"Archivo CAV":{"0":"CAMILA_SEGUEL_-_CAV.pdf"},"Errores CAV":{"0":""}}',
    }
    results = lambda_handler(event, "")

    assert results == expected_results


def test_processor_with_pagare():
    event = {
        "Records": [{"s3": {"object": {"key": "ExtractorDeudas/Archivo/Forum/Test/CAMILA_SEGUEL_-_PAGARE.json"}}}]
    }

    expected_results = {
        "document_type": "pagare",
        "key": "ExtractorDeudas/Archivo/Forum/Test/CAMILA_SEGUEL_-_PAGARE.pdf",
        "data": '{"Nombre y Apellido":{"0":"Camila Constanza Seguel Olivares"},"RUT":{"0":"17028624-3"},"Representante":{"0":null},"Rut Representante":{"0":null},"Direcci\\u00f3n":{"0":"Yerbas Buenas 27 Dpto 76"},"Comuna":{"0":"Los Andes"},"N\\u00ba Documento":{"0":1150427},"N\\u00ba Cuotas":{"0":36},"Tasa de Interes":{"0":0.0179},"Archivo Pagare":{"0":"CAMILA_SEGUEL_-_PAGARE.pdf"},"Errores Pagare":{"0":"Representante Legal, Rut Representante Legal"}}',
    }
    results = lambda_handler(event, "")

    assert results == expected_results


def test_processor_with_tabla():
    event = {"Records": [{"s3": {"object": {"key": "ExtractorDeudas/Archivo/Forum/Test/CAMILA_SEGUEL_-_TABLA.json"}}}]}

    expected_results = {
        "document_type": "tabla",
        "key": "ExtractorDeudas/Archivo/Forum/Test/CAMILA_SEGUEL_-_TABLA.pdf",
        "data": '{"RUT":{"0":"17028624-3"},"Vendedor":{"0":"LA852162"},"\\u00daltima Cuota Pagada":{"0":16},"Cuota Impaga":{"0":17},"Valor Cuota":{"0":301072},"Total Pagar\\u00e9":{"0":7839317},"Monto Impago":{"0":5009435},"Fecha Pago Primera Cuota":{"0":"2018-05-10T00:00:00.000Z"},"D\\u00eda de pago":{"0":10},"Fecha Mora":{"0":"2019-09-10T00:00:00.000Z"},"Valor \\u00daltima Cuota":{"0":301072},"Archivo Desarrollo Contratos":{"0":"CAMILA_SEGUEL_-_TABLA.pdf"},"Errores Desarrollo Contratos":{"0":""}}',
    }
    results = lambda_handler(event, "")

    assert results == expected_results


if __name__ == "__main__":
    test_processor_with_cav()
    test_processor_with_pagare()
    test_processor_with_tabla()
    print("\033[92mAll test OK\033[0m")
