"""Test datemath against an Elasticsearch instance"""
# pylint: disable=C0103,C0111
from datetime import timedelta, datetime
from curator_api.exceptions import ConfigurationError
from curator_api.helpers.datemath import parse_datemath
from . import CuratorTestCase

class TestParseDateMath(CuratorTestCase):
    def test_function_positive(self):
        test_string = u'<.prefix-{2001-01-01-13||+1h/h{YYYY-MM-dd-HH|-07:00}}-suffix>'
        expected = u'.prefix-2001-01-01-14-suffix'
        self.assertEqual(expected, parse_datemath(self.client, test_string))
    def test_assorted_datemaths(self):
        ymd = '%Y.%m.%d'
        now = datetime.utcnow()
        one = timedelta(days=1)
        ten = timedelta(days=10)
        offset = timedelta(hours=7)
        for test_string, expected in [
                (u'<prefix-{now}-suffix>', u'prefix-{0}-suffix'.format(now.strftime(ymd))),
                (u'<prefix-{now-1d/d}>', u'prefix-{0}'.format((now-one).strftime(ymd))),
                (u'<{now+1d/d}>', u'{0}'.format((now+one).strftime(ymd))),
                (u'<{now+10d/d{YYYY-MM}}>', u'{0}'.format((now+ten).strftime('%Y-%m'))),
                (u'<{now+10d/h{YYYY-MM-dd-HH|-07:00}}>',
                 u'{0}'.format((now+ten-offset).strftime('%Y-%m-%d-%H'))),
            ]:
            self.assertEqual(expected, parse_datemath(self.client, test_string))
