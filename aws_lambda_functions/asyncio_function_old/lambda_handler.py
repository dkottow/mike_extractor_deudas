import asyncio

from mike_ocr.main import main

KEY = 'ExtractorDeudas/Entrada/Forum/2021-01-11T20_03_11_360740/cav - copia.zip'


def handler(event, context):
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main(KEY))
    finally:
        loop.close()


if __name__ == "__main__":
    handler({}, {})