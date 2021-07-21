from src.main import main


def lambda_handler(event: dict, context):
    return main(event, context)
