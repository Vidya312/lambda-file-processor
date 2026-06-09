from lambda_function import lambda_handler

def test_lambda():
    event = {
        "bucket": "test-bucket",
        "file": "test.csv"
    }

    result = lambda_handler(event, None)
    assert result["statusCode"] == 200
