from abc import abstractmethod
from typing import Any, Dict
import pandas as pd


class Processor:
    @abstractmethod
    def process_response(self, response: Dict[str, Any], parsed_key: Dict[str, str]) -> pd.DataFrame:
        raise (NotImplementedError)
