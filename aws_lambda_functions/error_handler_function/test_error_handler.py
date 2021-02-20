from lambda_function import lambda_handler


def test_error_handler():
    event = {
        "version": "1.0",
        "timestamp": "2019-11-24T21:52:47.333Z",
        "requestContext": {
            "requestId": "8ea123e4-1db7-4aca-ad10-d9ca1234c1fd",
            "functionArn": "arn:aws:lambda:sa-east-1:123456678912:function:event-destinations:$LATEST",
            "condition": "RetriesExhausted",
            "approximateInvokeCount": 3,
        },
        "requestPayload": {"Success": False},
        "responseContext": {"statusCode": 200, "executedVersion": "$LATEST", "functionError": "Handled"},
        "responsePayload": {
            "errorMessage": "[ExtractorDeudas/Archivo/Forum/2021-01-11T20_03_11_360740/blablabla.nopdf] Error while trying to classify current file document type: is not a pdf file.",
            "errorType": "Error",
            "stackTrace": ["exports.handler (/var/task/index.js:18:18)"],
        },
    }

    assert lambda_handler(event, "") == {
        "key": "ExtractorDeudas/Archivo/Forum/2021-01-11T20_03_11_360740/blablabla.nopdf",
        "error": "Error while trying to classify current file document type: is not a pdf file.",
        "event": event,
    }


if __name__ == "__main__":
    test_error_handler()
    print("\033[92mAll test OK\033[0m")
