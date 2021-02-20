from abc import abstractmethod
from typing import Dict, List, Union
import pandas as pd


class Processor:
    @abstractmethod
    def process_response(self, text: str, tables: Union[List[pd.DataFrame], None],
                         forms: Union[List, Dict, None], filename: str, *args,
                         **kwargs) -> pd.DataFrame:
        raise (NotImplementedError)
