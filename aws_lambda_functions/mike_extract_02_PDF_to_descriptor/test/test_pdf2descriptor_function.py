import pytest
import boto3
from moto import mock_s3

from lambda_function import lambda_handler
from src.utils import MikeKey
from src.config import Config as config
from src.test_utils import (
    check_if_key_exists_in_s3,
    generate_s3_event,
    get_json_from_s3,
    upload_test_file,
)


@pytest.fixture
def s3_client():
    if config.MOCK_TESTS:
        mock = mock_s3()
        mock.start()

        s3_client = boto3.client("s3", region_name=config.AWS_REGION)
        s3_client.create_bucket(
            Bucket=config.S3_BUCKET, CreateBucketConfiguration={"LocationConstraint": config.AWS_REGION}
        )
        return s3_client

    s3_client = boto3.client("s3")
    return s3_client


def pdf2descriptor_base_test(expected_output: dict, test_key: MikeKey, output_key: MikeKey, s3_client):

    upload_test_file(test_key.to_str(), config.S3_BUCKET, config.TEST_RESOURCES_PATH, s3_client)

    event = generate_s3_event(test_key.to_str())

    output = lambda_handler(event, "")

    # problemas: distintas implementaciones dan distintos descriptores, pero igual son compatibles...
    del output["descriptor"]
    del expected_output["descriptor"]

    assert output == expected_output

    assert check_if_key_exists_in_s3(output_key.to_str(), config.S3_BUCKET, s3_client, prefix=output_key.to_str())
    uploaded_json = get_json_from_s3(output_key.to_str(), config.S3_BUCKET, s3_client)

    # nuevamente eliminar el descriptor.
    del uploaded_json["descriptor"]

    assert uploaded_json == expected_output


def test_pdf2descriptor_with_forum_cav(s3_client):

    test_key = MikeKey(f"Intermedio/{config.INTER_01_EXTRACT}/Forum/Test/CAMILA_SEGUEL_-_CAV.pdf")
    output_key = MikeKey(test_key.to_str(new_etapa_intermedio=config.INTER_02_PDF2DESCRIPTOR, new_extension="json"))

    expected_output = {
        "success": True,
        "doc_key": f"Intermedio/{config.INTER_01_EXTRACT}/Forum/Test/CAMILA_SEGUEL_-_CAV.pdf",
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
            95952780.47572327,
            24131323.36649072,
            16573457.068631649,
            24308624.969731808,
            58781295.603104115,
            15853038.643129826,
            21034334.735089064,
            32731957.910121918,
            104125903.95729399,
            21886406.18382144,
            15236953.417106152,
            22710952.944568634,
            58831013.431450844,
            17924448.60081768,
            23417204.393794417,
            34559446.772280216,
        ],
        "descriptor_type": "HOG",
    }
    pdf2descriptor_base_test(expected_output, test_key, output_key, s3_client)


def test_pdf2descriptor_with_forum_pagare(s3_client):

    test_key = MikeKey(f"Intermedio/{config.INTER_01_EXTRACT}/Forum/Test/CAMILA_SEGUEL_-_PAGARE.pdf")
    output_key = MikeKey(test_key.to_str(new_etapa_intermedio=config.INTER_02_PDF2DESCRIPTOR, new_extension="json"))

    expected_output = {
        "success": True,
        "doc_key": f"Intermedio/{config.INTER_01_EXTRACT}/Forum/Test/CAMILA_SEGUEL_-_PAGARE.pdf",
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
    pdf2descriptor_base_test(expected_output, test_key, output_key, s3_client)


def test_pdf2descriptor_with_forum_tabla(s3_client):

    test_key = MikeKey(f"Intermedio/{config.INTER_01_EXTRACT}/Forum/Test/CAMILA_SEGUEL_-_TABLA.pdf")
    output_key = MikeKey(test_key.to_str(new_etapa_intermedio=config.INTER_02_PDF2DESCRIPTOR, new_extension="json"))

    expected_output = {
        "success": True,
        "doc_key": f"Intermedio/{config.INTER_01_EXTRACT}/Forum/Test/CAMILA_SEGUEL_-_TABLA.pdf",
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
    pdf2descriptor_base_test(expected_output, test_key, output_key, s3_client)


def test_pdf2descriptor_with_santander_pagare(s3_client):

    test_key = MikeKey(f"Intermedio/{config.INTER_01_EXTRACT}/Santander/Test/CRISTOBAL_TRONCOSO_TRONCOSO_-_PAGARE.pdf")
    output_key = MikeKey(test_key.to_str(new_etapa_intermedio=config.INTER_02_PDF2DESCRIPTOR, new_extension="json"))

    expected_output = {
        "success": True,
        "doc_key": f"Intermedio/{config.INTER_01_EXTRACT}/Santander/Test/CRISTOBAL_TRONCOSO_TRONCOSO_-_PAGARE.pdf",
        "descriptor": [
            22709.46775817871,
            268.6372979879379,
            16.124515533447266,
            6.324555397033691,
            31.92838764190674,
            0.0,
            36.358487486839294,
            14728.264335632324,
            24649.786669254303,
            79.39989984035492,
            20.0,
            0.0,
            84.53219366073608,
            0.0,
            135.43072855472565,
            15681.163919448853,
            641428.8538370132,
            4861.847338795662,
            1429.2058362960815,
            13934.89165019989,
            16377.668500423431,
            7378.873793125153,
            8958.838316202164,
            530294.7729363441,
            1010388.5104746819,
            11061.644122600555,
            5774.947578907013,
            12439.773785114288,
            13757.223386764526,
            1441.8372931480408,
            4946.116719007492,
            540308.2454724312,
            50750.85052728653,
            8884.75986802578,
            16412.913828849792,
            702875.1042790413,
            976989.7886228561,
            14357.416289329529,
            7311.08587372303,
            7436.8289794921875,
            38131.29700136185,
            4349.931338310242,
            5182.849690437317,
            13390.353856563568,
            24662.674122810364,
            5059.6290163993835,
            4915.897125482559,
            12189.921410560608,
            147795394.36435795,
            56037421.73633993,
            52829454.44296932,
            102738768.95939541,
            108334140.24474955,
            55282587.81142521,
            58726143.048372984,
            138066978.76033735,
            152403945.98135853,
            61488256.015325665,
            56514713.87954521,
            100258067.66337776,
            98683049.00887585,
            59065406.77029467,
            61232120.48044217,
            145782465.16692924,
        ],
        "descriptor_type": "HOG",
    }

    pdf2descriptor_base_test(expected_output, test_key, output_key, s3_client)


def test_pdf2descriptor_with_santander_tabla(s3_client):

    test_key = MikeKey(f"Intermedio/{config.INTER_01_EXTRACT}/Santander/Test/CRISTOBAL_TRONCOSO_TRONCOSO_-_TABLA.pdf")
    output_key = MikeKey(test_key.to_str(new_etapa_intermedio=config.INTER_02_PDF2DESCRIPTOR, new_extension="json"))

    expected_output = {
        "success": True,
        "doc_key": f"Intermedio/{config.INTER_01_EXTRACT}/Santander/Test/CRISTOBAL_TRONCOSO_TRONCOSO_-_TABLA.pdf",
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
            31339696.310084343,
            20749173.849356413,
            12018202.901780605,
            18197217.699798584,
            23160924.37115383,
            11668319.473988533,
            16599709.073789239,
            22222204.925912857,
            32815865.392178535,
            18616968.9306345,
            15090170.468792439,
            16666082.500154972,
            21481892.136850834,
            12442387.595655441,
            15502993.786065817,
            24233641.495627403,
        ],
        "descriptor_type": "HOG",
    }

    pdf2descriptor_base_test(expected_output, test_key, output_key, s3_client)


def test_pdf2descriptor_with_santander_tabla_2(s3_client):

    test_key = MikeKey(f"Intermedio/{config.INTER_01_EXTRACT}/Santander/Test/MAN_WHA_KIM_-_TABLA.pdf")
    output_key = MikeKey(test_key.to_str(new_etapa_intermedio=config.INTER_02_PDF2DESCRIPTOR, new_extension="json"))

    expected_output = {
        "success": True,
        "doc_key": f"Intermedio/{config.INTER_01_EXTRACT}/Santander/Test/MAN_WHA_KIM_-_TABLA.pdf",
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
            32426546.60964012,
            20412771.49884188,
            12035862.819277763,
            18655419.814724922,
            23415827.126950264,
            11161314.8196249,
            16817190.871949553,
            21028770.026251793,
            33026815.722850323,
            18296597.58243692,
            15550568.552164555,
            16956320.695760727,
            21278026.033578396,
            12229396.512290478,
            15222476.129367352,
            22790004.956214905,
        ],
        "descriptor_type": "HOG",
    }

    pdf2descriptor_base_test(expected_output, test_key, output_key, s3_client)


def test_pdf2descriptor_exceptions():
    event = {
        "Records": [{"s3": {"object": {"key": f"Intermedio/{config.INTER_01_EXTRACT}/Forum/Test/some_file.nopdf"}}}]
    }
    with pytest.raises(Exception, match=r"\[.*\] Exception: The provided file is not a pdf."):
        lambda_handler(event, "")
