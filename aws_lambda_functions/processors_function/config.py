from forum.cav import CAVProcessor
from forum.pagare import PagareProcessor
from forum.desarolllo_contratos import DesarrolloContratosProcessor


class Config:
    PROCESSORS = {"cav": CAVProcessor, "pagare": PagareProcessor, "tabla": DesarrolloContratosProcessor}
    S3_BUCKET = "textract-console-us-east-2-7e1edcda-ec50-4013-9762-a06e7b200006"
