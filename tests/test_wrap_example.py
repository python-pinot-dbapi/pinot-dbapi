# -*- coding: utf-8 -*-

import importlib

from typing import *

from testing_context import TestCase


class ExampleWrapper(TestCase):

    test_examples: List[str] = ["pinot_quickstart_auth_zk", "pinot_quickstart_batch", "pinot_async"]

    def test_run_example(self) -> None:
        for test_example in self.test_examples:
            test_module = importlib.import_module("examples." + test_example, ".")
            test_module.run_main()


