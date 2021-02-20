from ..cav import CAVProcessor
from .cav_examples import examples


def test_CAVProcessor():

    for exmaple in examples:

        processor = CAVProcessor(exmaple['text'], exmaple['tables'], exmaple['forms'],
                                 exmaple['filename'])

        results = processor.process_response()
        results = results.to_dict(orient='records')[0]

        assert exmaple['expected_results'] == results
