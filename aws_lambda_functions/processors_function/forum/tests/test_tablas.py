import pandas as pd

from ..desarolllo_contratos import DesarrolloContratosProcessor
from .tablas_examples import examples


def test_DDAProcessor():

    for example in examples:

        processor = DesarrolloContratosProcessor(
            example['text'], [pd.DataFrame(t) for t in example['tables']],
            example['forms'], example['filename'])

        results = processor.process_response()
        results = results.to_dict(orient='records')[0]

        for key, expected_val in example['expected_results'].items():
            assert results[key] == expected_val
