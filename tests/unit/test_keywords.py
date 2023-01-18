from unittest import TestCase

from pinotdb.keywords import CALCITE_KEYWORDS, SUPERSET_KEYWORDS


class CalciteKeywordsTest(TestCase):
    def test_contains_some_keywords(self):
        self.assertIn('ARRAY', CALCITE_KEYWORDS)


class SupersetKeywordsTest(TestCase):
    def test_contains_some_keywords(self):
        self.assertIn('__timestamp', SUPERSET_KEYWORDS)
