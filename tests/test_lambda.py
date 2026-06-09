from lambda_function import lambda_handler

def test_lambda():
    event = {
        "bucket": "filepipeline-092042969531-raw",
        "file": "test.txt.txt"
    }

    result = lambda_handler(event, None)
    assert result["statusCode"] == 200
