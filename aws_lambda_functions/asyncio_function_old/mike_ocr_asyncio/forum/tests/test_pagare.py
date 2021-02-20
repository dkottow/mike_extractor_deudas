from ..pagare import PagareProcessor
from .pagare_examples import examples


def test_PagareProcessor():

    for example in examples:

        processor = PagareProcessor(example['text'], example['tables'], example['forms'],
                                    example['filename'])

        results = processor.process_response()
        results = results.to_dict(orient='records')[0]

        for key, expected_val in example['expected_results'].items():
            assert results[key] == expected_val
