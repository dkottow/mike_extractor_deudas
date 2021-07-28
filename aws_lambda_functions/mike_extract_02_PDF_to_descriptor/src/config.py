class Config:
    # AWS
    AWS_FUNCTION_NAME = "mike-extract-02-pdf_to_descriptor"  # also change in deploy.bash
    AWS_REGION = "sa-east-1"
    S3_BUCKET = "mike-extract-s3-system-prod"

    # intermedio paths
    INTER_01_EXTRACT = "01_Extract"
    INTER_02_PDF2DESCRIPTOR = "02_PDF2Descriptor"
    INTER_03_CLASSIFICATION = "03_Classification"
    INTER_04_TEXTRACT = "04_Textract"
    INTER_05_PROCESSORS = "05_Processors"

    # testing
    MOCK_TESTS = True
    TEST_RESOURCES_PATH = "./test/test_resources"
