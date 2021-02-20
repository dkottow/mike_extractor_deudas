import pytest
from lambda_function import lambda_handler


def test_pdf_to_descriptor_with_cav():

    event = {"Records": [{"s3": {"object": {"key": "ExtractorDeudas/Archivo/Forum/Test/CAMILA_SEGUEL_-_CAV.pdf"}}}]}

    expected_output = {
        "key": "ExtractorDeudas/Archivo/Forum/Test/CAMILA_SEGUEL_-_CAV.pdf",
        "descriptor": [
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            76161064.21380997,
            20804729.111046314,
            16888431.32515526,
            20988507.47180128,
            53026044.36100674,
            15364990.763726234,
            19636315.803418398,
            24133952.159643173,
            81240172.86366987,
            18651996.708309293,
            16549060.078660965,
            19377738.70054388,
            53096443.096756935,
            17588073.48936081,
            21409221.818852544,
            24190044.398444653,
        ],
        "descriptor_type": "HOG",
    }
    assert lambda_handler(event, "") == expected_output


def test_pdf_to_descriptor_with_pagare():
    event = {"Records": [{"s3": {"object": {"key": "ExtractorDeudas/Archivo/Forum/Test/CAMILA_SEGUEL_-_PAGARE.pdf"}}}]}
    expected_output = {
        "key": "ExtractorDeudas/Archivo/Forum/Test/CAMILA_SEGUEL_-_PAGARE.pdf",
        "descriptor": [
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            2699.0348834991455,
            456.4125065803528,
            71.5541763305664,
            214.5205535888672,
            2500.24982881546,
            689.2807264328003,
            27116.66237938404,
            167879.20409822464,
            2971819.255757332,
            2635.127676486969,
            706.7932705879211,
            20470.83077812195,
            11935.735500335693,
            31.304952144622803,
            366.85806465148926,
            173.9252734184265,
            20700.881988048553,
            9488.8557959795,
            14043.199035167694,
            335510.25941848755,
            1025125.0169377327,
            21522.22985124588,
            6974.988359332085,
            3392.9381890296936,
            15882.066977024078,
            11044.384843587875,
            33890.80087375641,
            731813.7668166161,
            2849465.4636039734,
            6226.4639258384705,
            8574.425882101059,
            37849.38393497467,
            171400088.24581146,
            66324234.580191135,
            35739038.70597601,
            64091416.14903164,
            88325195.09902239,
            47867267.571941376,
            79880396.41067672,
            105397137.82951736,
            147902036.62310028,
            70616240.40475643,
            35819563.325286865,
            54573021.37293625,
            95552160.1586523,
            41963300.243083,
            83090765.12121594,
            89402079.44763327,
        ],
        "descriptor_type": "HOG",
    }
    assert lambda_handler(event, "") == expected_output


def test_pdf_to_descriptor_with_tabla():
    event = {"Records": [{"s3": {"object": {"key": "ExtractorDeudas/Archivo/Forum/Test/CAMILA_SEGUEL_-_TABLA.pdf"}}}]}
    expected_output = {
        "key": "ExtractorDeudas/Archivo/Forum/Test/CAMILA_SEGUEL_-_TABLA.pdf",
        "descriptor": [
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            190668.70706176758,
            4923.218467712402,
            0.0,
            0.0,
            48515.16398668289,
            0.0,
            9.899494528770447,
            6.324555397033691,
            8.0,
            0.0,
            0.0,
            0.0,
            48524.0,
            0.0,
            6640.704780578613,
            99222.98037719727,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            68303149.33230925,
            28879672.916467547,
            21118449.197689056,
            137404397.41776705,
            254149913.0065217,
            20813507.91753578,
            25410865.469547033,
            45307346.99019194,
            68513462.86330223,
            30541196.300249457,
            26705166.344163418,
            130778685.70421743,
            255835529.74632645,
            20343330.935175896,
            25255833.01041329,
            51234549.695584774,
        ],
        "descriptor_type": "HOG",
    }
    assert lambda_handler(event, "") == expected_output


def test_pdf_to_descriptor_exceptions():
    event = {"Records": [{"s3": {"object": {"key": "ExtractorDeudas/Archivo/Forum/Test/blablabla.nopdf"}}}]}
    with pytest.raises(
        Exception, match=r".*Error while trying to classify current file document type: is not a pdf file.*"
    ):
        lambda_handler(event, "")

    event = {"Records": [{"s3": {"object": {"key": "ExtractorDeudas/Archivo/Forum/Test/no_correct_pdf.pdf"}}}]}
    with pytest.raises(Exception, match=r".*Unexpected Exception in pdf to descriptor.*"):
        lambda_handler(event, "")

    event = {"Records": [{"s3": {"object": {"key": "path_mal_formada"}}}]}
    with pytest.raises(Exception, match=r".*Error while trying to parse.*"):
        lambda_handler(event, "")


if __name__ == "__main__":
    test_pdf_to_descriptor_with_cav()
    test_pdf_to_descriptor_with_pagare()
    test_pdf_to_descriptor_with_tabla()
    test_pdf_to_descriptor_exceptions()
    print("\033[92mAll test OK\033[0m")
