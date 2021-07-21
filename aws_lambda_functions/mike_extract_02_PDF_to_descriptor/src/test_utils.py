import json
from typing import Union
from pathlib import Path
from src.utils import MikeKey


def generate_s3_event(key: Union[str, MikeKey]):
    return {"Records": [{"s3": {"object": {"key": key}}}]}


# ---------------------------------------------------------------------------------------------------------------------
def upload_test_file(key: str, bucket: str, test_resources_path: str, s3_client):
    # Subir entrada al bucket
    with open(f"{test_resources_path}/{key}", "rb") as reader:
        test_file = reader.read()
        s3_client.put_object(
            Body=test_file,
            Bucket=bucket,
            Key=key,
        )


def upload_json_files_to_s3(s3_client, bucket: str, files_path: str):
    test_file_path = Path(files_path).rglob("*.json")
    res_and_err_files = [x for x in test_file_path]
    for key in res_and_err_files:
        with open(key, encoding="utf-8") as data:
            new_key = str(key).replace("test/test_resources/", "")
            s3_client.put_object(
                Body=data.read().encode(encoding="UTF-8", errors="ignore"),
                Key=new_key,
                Bucket=bucket,
            )


def upload_pdf_files_to_s3(s3_client, bucket: str, files_path: str):
    # Subir archivos de clasificación a s3
    test_file_path = Path(files_path).rglob("*.pdf")
    res_and_err_files = [x for x in test_file_path]
    for key in res_and_err_files:
        new_key = str(key).replace("test/test_resources/", "")
        s3_client.upload_file(
            Filename=str(key),
            Key=new_key,
            Bucket=bucket,
        )


# ---------------------------------------------------------------------------------------------------------------------
def list_keys(s3_client, bucket: str, prefix: Union[str, None] = None):
    try:
        if isinstance(prefix, str):
            filelist = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)["Contents"]
        else:
            filelist = s3_client.list_objects_v2(Bucket=bucket)["Contents"]

        return [o["Key"] for o in filelist]

    except Exception as e:
        print(e)
        return []


# ---------------------------------------------------------------------------------------------------------------------
def check_if_key_exists_in_s3(key: str, bucket: str, s3_client, prefix: Union[str, None] = None):
    key_list = list_keys(s3_client, bucket, prefix)
    return key in key_list


# ---------------------------------------------------------------------------------------------------------------------
def get_json_from_s3(key: str, bucket: str, s3_client):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    json_bytes = response["Body"].read()
    return json.loads(json_bytes)
