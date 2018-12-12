"""Test utility functions"""
# pylint: disable=C0103,C0111
from unittest import TestCase
from curator_api.helpers.utils import byte_size, check_csv, ensure_list, prune_nones, to_csv

class TestByteSize(TestCase):
    def test_byte_size(self):
        size = 3*1024*1024*1024*1024*1024*1024*1024
        unit = ['Z', 'E', 'P', 'T', 'G', 'M', 'K', '']
        for i in range(0, 7):
            self.assertEqual('3.0{0}B'.format(unit[i]), byte_size(size))
            size /= 1024
    def test_byte_size_yotta(self):
        size = 3*1024*1024*1024*1024*1024*1024*1024*1024
        self.assertEqual('3.0YB', byte_size(size))
    def test_raise_invalid(self):
        self.assertRaises(TypeError, byte_size, 'invalid')

class TestEnsureList(TestCase):
    def test_ensure_list_returns_lists(self):
        for values in [
                (["a", "b", "c", "d"], ["a", "b", "c", "d"]),
                ("abcd", ["abcd"]),
                ([["abcd", "defg"], 1, 2, 3], [["abcd", "defg"], 1, 2, 3]),
                ({"a":"b", "c":"d"}, [{"a":"b", "c":"d"}]),
            ]:
            pre = values[0]
            post = values[1]
            self.assertEqual(post, ensure_list(pre))

class TestTo_CSV(TestCase):
    def test_to_csv_will_return_csv(self):
        for values in [
                (["a", "b", "c", "d"], "a,b,c,d"),
                (["a"], "a"),
            ]:
            pre = values[0]
            post = values[1]
            self.assertEqual(post, to_csv(pre))
    def test_to_csv_will_return_None(self):
        self.assertIsNone(to_csv([]))

class TestCheckCSV(TestCase):
    def test_check_csv_positive(self):
        self.assertTrue(check_csv("1,2,3"))
    def test_check_csv_negative(self):
        self.assertFalse(check_csv("12345"))
    def test_check_csv_list(self):
        self.assertTrue(check_csv(["1", "2", "3"]))
    def test_check_csv_unicode(self):
        self.assertFalse(check_csv(u'test'))
    def test_check_csv_wrong_value(self):
        self.assertRaises(TypeError, check_csv, 123)

class TestPruneNones(TestCase):
    def test_prune_samples(self):
        for values in [
                ({}, {'a': None}),
                ({'foo': 'bar'}, {'foo': 'bar'}),
            ]:
            expected = values[0]
            testval = values[1]
            self.assertEqual(expected, prune_nones(testval))
