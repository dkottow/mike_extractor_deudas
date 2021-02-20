import pytest
from lambda_function import lambda_handler


def test_classification_with_CAV():

    event = {
        "version": "1.0",
        "timestamp": "2021-01-20T22:00:58.720Z",
        "requestContext": {
            "requestId": "3d6dbba0-ed17-48cc-ab81-1a1ecf0c298b",
            "functionArn": "arn:aws:lambda:us-east-2:454177333545:function:Mike_OCR_PDF_To_Descriptor:$LATEST",
            "condition": "Success",
            "approximateInvokeCount": 1,
        },
        "requestPayload": {
            "Records": [
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "awsRegion": "us-east-2",
                    "eventTime": "2021-01-20T22:00:50.452Z",
                    "eventName": "ObjectCreated:Put",
                    "userIdentity": {"principalId": "AWS:AROAWTPYUEEUQM3LJFI6F:Mike_OCR_Map"},
                    "requestParameters": {"sourceIPAddress": "3.140.199.2"},
                    "responseElements": {
                        "x-amz-request-id": "01BDDA64A6566AB6",
                        "x-amz-id-2": "rn+rY2kH6sJb4VuIpiBqsaNb/pezRnzTGwekX2ZzEy5ydryeQV9tQd1xwFRBf88PFMvZXwR1DpJCmHrK2y6yBR4ETGTO/P35",
                    },
                    "s3": {
                        "s3SchemaVersion": "1.0",
                        "configurationId": "4cdd5e97-9790-4cb6-bb9c-fa4648ad18cf",
                        "bucket": {
                            "name": "textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006",
                            "ownerIdentity": {"principalId": "AFTBJ9N16F5MT"},
                            "arn": "arn:aws:s3:::textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006",
                        },
                        "object": {
                            "key": "ExtractorDeudas/Archivo/Forum/2021-01-20T22_00_52_875730/CAMILA_SEGUEL_-_CAV.pdf",
                            "size": 47395,
                            "eTag": "3cc458c4c5ba498b93005268ef061cac",
                            "sequencer": "006008A8174A4310A0",
                        },
                    },
                }
            ]
        },
        "responseContext": {"statusCode": 200, "executedVersion": "$LATEST"},
        "responsePayload": {
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
        },
    }

    expected_output = {
        "key": "ExtractorDeudas/Archivo/Forum/Test/CAMILA_SEGUEL_-_CAV.pdf",
        "document_type": "cav",
    }
    assert lambda_handler(event, "") == expected_output


def test_classification_with_pagare():

    event = {
        "version": "1.0",
        "timestamp": "2021-01-20T22:00:58.720Z",
        "requestContext": {
            "requestId": "3d6dbba0-ed17-48cc-ab81-1a1ecf0c298b",
            "functionArn": "arn:aws:lambda:us-east-2:454177333545:function:Mike_OCR_PDF_To_Descriptor:$LATEST",
            "condition": "Success",
            "approximateInvokeCount": 1,
        },
        "requestPayload": {
            "Records": [
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "awsRegion": "us-east-2",
                    "eventTime": "2021-01-20T22:00:50.452Z",
                    "eventName": "ObjectCreated:Put",
                    "userIdentity": {"principalId": "AWS:AROAWTPYUEEUQM3LJFI6F:Mike_OCR_Map"},
                    "requestParameters": {"sourceIPAddress": "3.140.199.2"},
                    "responseElements": {
                        "x-amz-request-id": "01BDDA64A6566AB6",
                        "x-amz-id-2": "rn+rY2kH6sJb4VuIpiBqsaNb/pezRnzTGwekX2ZzEy5ydryeQV9tQd1xwFRBf88PFMvZXwR1DpJCmHrK2y6yBR4ETGTO/P35",
                    },
                    "s3": {
                        "s3SchemaVersion": "1.0",
                        "configurationId": "4cdd5e97-9790-4cb6-bb9c-fa4648ad18cf",
                        "bucket": {
                            "name": "textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006",
                            "ownerIdentity": {"principalId": "AFTBJ9N16F5MT"},
                            "arn": "arn:aws:s3:::textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006",
                        },
                        "object": {
                            "key": "ExtractorDeudas/Archivo/Forum/2021-01-20T22_00_52_875730/CAMILA_SEGUEL_-_CAV.pdf",
                            "size": 47395,
                            "eTag": "3cc458c4c5ba498b93005268ef061cac",
                            "sequencer": "006008A8174A4310A0",
                        },
                    },
                }
            ]
        },
        "responseContext": {"statusCode": 200, "executedVersion": "$LATEST"},
        "responsePayload": {
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
        },
    }

    expected_output = {
        "key": "ExtractorDeudas/Archivo/Forum/Test/CAMILA_SEGUEL_-_PAGARE.pdf",
        "document_type": "pagare",
    }
    assert lambda_handler(event, "") == expected_output


def test_classification_with_tabla():

    event = {
        "version": "1.0",
        "timestamp": "2021-01-20T22:00:58.720Z",
        "requestContext": {
            "requestId": "3d6dbba0-ed17-48cc-ab81-1a1ecf0c298b",
            "functionArn": "arn:aws:lambda:us-east-2:454177333545:function:Mike_OCR_PDF_To_Descriptor:$LATEST",
            "condition": "Success",
            "approximateInvokeCount": 1,
        },
        "requestPayload": {
            "Records": [
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "awsRegion": "us-east-2",
                    "eventTime": "2021-01-20T22:00:50.452Z",
                    "eventName": "ObjectCreated:Put",
                    "userIdentity": {"principalId": "AWS:AROAWTPYUEEUQM3LJFI6F:Mike_OCR_Map"},
                    "requestParameters": {"sourceIPAddress": "3.140.199.2"},
                    "responseElements": {
                        "x-amz-request-id": "01BDDA64A6566AB6",
                        "x-amz-id-2": "rn+rY2kH6sJb4VuIpiBqsaNb/pezRnzTGwekX2ZzEy5ydryeQV9tQd1xwFRBf88PFMvZXwR1DpJCmHrK2y6yBR4ETGTO/P35",
                    },
                    "s3": {
                        "s3SchemaVersion": "1.0",
                        "configurationId": "4cdd5e97-9790-4cb6-bb9c-fa4648ad18cf",
                        "bucket": {
                            "name": "textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006",
                            "ownerIdentity": {"principalId": "AFTBJ9N16F5MT"},
                            "arn": "arn:aws:s3:::textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006",
                        },
                        "object": {
                            "key": "ExtractorDeudas/Archivo/Forum/2021-01-20T22_00_52_875730/CAMILA_SEGUEL_-_CAV.pdf",
                            "size": 47395,
                            "eTag": "3cc458c4c5ba498b93005268ef061cac",
                            "sequencer": "006008A8174A4310A0",
                        },
                    },
                }
            ]
        },
        "responseContext": {"statusCode": 200, "executedVersion": "$LATEST"},
        "responsePayload": {
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
        },
    }

    expected_output = {
        "key": "ExtractorDeudas/Archivo/Forum/Test/CAMILA_SEGUEL_-_TABLA.pdf",
        "document_type": "tabla",
    }
    assert lambda_handler(event, "") == expected_output


def test_classification_with_sinacofi():
    event = {
        "version": "1.0",
        "timestamp": "2021-01-20T22:00:58.720Z",
        "requestContext": {
            "requestId": "3d6dbba0-ed17-48cc-ab81-1a1ecf0c298b",
            "functionArn": "arn:aws:lambda:us-east-2:454177333545:function:Mike_OCR_PDF_To_Descriptor:$LATEST",
            "condition": "Success",
            "approximateInvokeCount": 1,
        },
        "requestPayload": {
            "Records": [
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "awsRegion": "us-east-2",
                    "eventTime": "2021-01-20T22:00:50.452Z",
                    "eventName": "ObjectCreated:Put",
                    "userIdentity": {"principalId": "AWS:AROAWTPYUEEUQM3LJFI6F:Mike_OCR_Map"},
                    "requestParameters": {"sourceIPAddress": "3.140.199.2"},
                    "responseElements": {
                        "x-amz-request-id": "01BDDA64A6566AB6",
                        "x-amz-id-2": "rn+rY2kH6sJb4VuIpiBqsaNb/pezRnzTGwekX2ZzEy5ydryeQV9tQd1xwFRBf88PFMvZXwR1DpJCmHrK2y6yBR4ETGTO/P35",
                    },
                    "s3": {
                        "s3SchemaVersion": "1.0",
                        "configurationId": "4cdd5e97-9790-4cb6-bb9c-fa4648ad18cf",
                        "bucket": {
                            "name": "textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006",
                            "ownerIdentity": {"principalId": "AFTBJ9N16F5MT"},
                            "arn": "arn:aws:s3:::textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006",
                        },
                        "object": {
                            "key": "ExtractorDeudas/Archivo/Forum/2021-01-20T22_00_52_875730/CAMILA_SEGUEL_-_CAV.pdf",
                            "size": 47395,
                            "eTag": "3cc458c4c5ba498b93005268ef061cac",
                            "sequencer": "006008A8174A4310A0",
                        },
                    },
                }
            ]
        },
        "responseContext": {"statusCode": 200, "executedVersion": "$LATEST"},
        "responsePayload": {
            "key": "ExtractorDeudas/Archivo/Forum/2021-01-27T00_35_40_503877/sinacofi_LB139997.pdf",
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
                53423647.532594204,
                12814452.551854134,
                10678969.826182365,
                16406106.688893795,
                107552320.12891245,
                14087512.337381363,
                19282752.855041623,
                28391734.812411785,
                54623598.40063381,
                15114206.416968822,
                12051075.387658596,
                15588085.403390884,
                104600608.15166187,
                14311827.068421364,
                19937430.650901914,
                31726735.79551077,
            ],
            "descriptor_type": "HOG",
        },
    }

    expected_output = {
        "key": "ExtractorDeudas/Archivo/Forum/Test/CAMILA_SEGUEL_-_TABLA.pdf",
        "document_type": "tabla",
    }
    assert lambda_handler(event, "") == expected_output


def test_exception():
    event = {
        "responsePayload": {
            "descriptor": [
                0.0,
                0.0,
            ],
            "descriptor_type": "HOG",
            "key": "ExtractorDeudas/Archivo/Forum/Test/CAMILA_SEGUEL_-_TABLA.pdf",
        }
    }
    with pytest.raises(Exception, match=r".*Unexpected exception in document classifier.*"):
        lambda_handler(event, "")


if __name__ == "__main__":
    # test_classification_with_sinacofi()
    test_classification_with_CAV()
    test_classification_with_pagare()
    test_classification_with_tabla()
    # test_exception()
    print("\033[92mAll test OK\033[0m")
