import importlib
import io
import os
import unittest

from contextlib import redirect_stdout, redirect_stderr
from parameterized import parameterized
from typing import *


_parameterized_test_scripts: List[str] = list()
skipped_modules: List[str] = [
    'pinot_live'  # pinot live is no longer available for testing
]

example_module = importlib.import_module("examples")
for example_script in os.listdir(example_module.__path__[0]):
    if example_script.endswith(".py") and not example_script.startswith("__"):
        _parameterized_test_scripts.append(example_script[:-3])


class ExampleWrappedTestCase(unittest.TestCase):

    @parameterized.expand(
        _parameterized_test_scripts
    )
    def test_run_example(self, example_module: str) -> None:
        test_module = importlib.import_module("examples." + example_module, ".")
        if example_module in skipped_modules:
            print("==================================")
            print(" Skipping test for example: " + test_module.__name__)
            print("==================================")
        else:
            print("==================================")
            print(" Running PinotDB Example Test against: " + test_module.__name__)
            print("==================================")
            # Piping stdio from example to test IO so it can be print out properly.
            with io.StringIO() as std_out, io.StringIO() as std_err:
                with redirect_stdout(std_out), redirect_stderr(std_err):
                    test_module.run_main()
                std_out_result = std_out.getvalue()
                # Skipping after 5 lines of stdio
                print(std_out_result.split("\n")[:5])
                print("...")
                std_err_result = std_err.getvalue()
                assert len(std_err_result) == 0 or std_err_result.find("error") == -1
