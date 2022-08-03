# -*- coding: utf-8 -*-

import importlib

from typing import *

from testing_context import TestCase


class ExampleWrapper(TestCase):

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
            test_module.run_main()


