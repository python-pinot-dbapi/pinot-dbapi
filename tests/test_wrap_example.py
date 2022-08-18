# -*- coding: utf-8 -*-

import importlib
import io

from contextlib import redirect_stdout, redirect_stderr
from typing import *

from tests.testing_context import TestCase


class ExampleWrapper(TestCase):

    # TODO: add rest of the example in
    # TODO: break each example into individual TestCase subclass
    test_examples: List[str] = ["pinot_quickstart_auth_zk", "pinot_quickstart_batch", "pinot_async"]

    # TODO: break this into multiple instantiable TestCase sub classes, one for each example file
    # This is useful when different table needs to be loaded from the pinot docker image by launching
    # subprocess.run('bin/pinot-admin.sh', 'LaunchDataIngestionJob', ...) to ingest tables
    def test_run_example(self) -> None:
        for test_example in self.test_examples:
            test_module = importlib.import_module("examples." + test_example, ".")
            print("==================================")
            print(" Running PinotDB Example Test against: " + test_module.__name__)
            print("==================================")
            with io.StringIO() as std_out, io.StringIO() as std_err:
                with redirect_stdout(std_out), redirect_stderr(std_err):
                    test_module.run_main()
                std_out_result = std_out.getvalue()
                print(std_out_result.split("\n")[:5])
                print("...")
                std_err_result = std_err.getvalue()
                assert len(std_err_result) == 0 or std_err_result.find("error") == -1
