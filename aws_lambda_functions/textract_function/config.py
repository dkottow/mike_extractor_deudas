class ProcessType:
    DETECTION = 1
    ANALYSIS = 2

class Config:
    S3_BUCKET = "textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006"
    SECONDS_TO_AWAIT_FOR_TEXTEXTRACT_RESPONSE = 20
    ROLE_ARN = "arn:aws:iam::454177333545:role/TextractRole"
    PROCESS_TYPE_BY_FILE_CLASS = {
        "cav": ProcessType.DETECTION,
        "pagare": ProcessType.DETECTION,
        "tabla": ProcessType.ANALYSIS,
    }

