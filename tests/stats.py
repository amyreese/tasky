import unittest

from collections import defaultdict

from tasky.stats import DictWrapper


class TestDictWrapper(unittest.TestCase):

    def test_dict(self):
        d = DictWrapper({})

        self.assertFalse(d)
        self.assertEqual(len(d), 0)
        self.assertNotIn('foo', d)

        d['foo'] = 'bar'
        self.assertTrue(d)
        self.assertEqual(len(d), 1)
        self.assertIn('foo', d)
        self.assertEqual(d['foo'], 'bar')

        def bad_key():
            return d['bar']

        self.assertRaises(KeyError, bad_key)

    def test_defaultdict(self):
        d = DictWrapper(defaultdict(int))

        self.assertFalse(d)
        self.assertEqual(len(d), 0)
        self.assertNotIn('foo', d)

        d['foo'] = 'bar'
        self.assertTrue(d)
        self.assertEqual(len(d), 1)
        self.assertIn('foo', d)
        self.assertEqual(d['foo'], 'bar')

        def bad_key():
            return d['bar']

        self.assertEqual(bad_key(), 0)

    def test_prefixes(self):
        d = {'foo': 'bar'}
        w = DictWrapper(d, 'prefix')

        self.assertIn('foo', d)
        self.assertEqual(d['foo'], 'bar')
        self.assertNotIn('foo', w)

        w['test'] = 100

        self.assertNotIn('test', d)
        self.assertIn('prefix.test', d)
        self.assertEqual(d['prefix.test'], 100)

        self.assertIn('test', w)
        self.assertNotIn('prefix.test', w)
        self.assertEqual(w['test'], 100)
