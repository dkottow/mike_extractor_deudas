from forum.cav import CAVProcessor
from forum.pagare import PagareProcessor
from forum.desarolllo_contratos import DesarrolloContratosProcessor

from forum.tests.cav_examples import examples as cav_examples
from forum.tests.pagare_examples import examples as pagare_examples
from forum.tests.tablas_examples import examples as tablas_examples

from utils import parse_mike_key


def test_processor(processor_class, examples):
    print(f"\u001b[34mTesting {processor_class} processor...\033[0m")
    processor = processor_class()
    for idx, example in enumerate(examples):
        results = processor.process_response(example["response"], parse_mike_key(example["response"]["key"]))
        results = results.to_dict(orient="records")[0]
        print(results, '\nTRUE\n', example["expected_results"])
        assert example["expected_results"] == results
        print(f"\033[92m\tExample {idx + 1} / {len(examples)} - OK\033[0m")


if __name__ == "__main__":
    test_processor(CAVProcessor, cav_examples)
    test_processor(PagareProcessor, pagare_examples)
    test_processor(DesarrolloContratosProcessor, tablas_examples)
    print("\033[92mAll test OK\033[0m")
