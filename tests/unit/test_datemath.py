"""Test datemath functions"""
# pylint: disable=C0103,C0111
import re
from datetime import datetime, timedelta
from unittest import TestCase
from mock import Mock
from elasticsearch.exceptions import NotFoundError
from curator_api.exceptions import ConfigurationError, MissingArgument
from curator_api.helpers.datemath import (
    absolute_date_range, date_range, datetime_to_epoch, fix_epoch, get_date_regex, get_datemath,
    get_datetime, get_point_of_reference, get_unit_count_from_name, isdatemath, parse_date_pattern,
    parse_datemath, TimestringSearch
)

EPOCH = datetime_to_epoch(datetime(2017, 4, 3, 22, 50, 17))

def make_epoch(year, month, day, hour, minute, second):
    return datetime_to_epoch(datetime(year, month, day, hour, minute, second))

class TestGetIndexTime(TestCase):
    def test_get_datetime(self):
        for text, datestring, dt in [
                ('2014.01.19', '%Y.%m.%d', datetime(2014, 1, 19)),
                ('14.01.19', '%y.%m.%d', datetime(2014, 1, 19)),
                ('2014-01-19', '%Y-%m-%d', datetime(2014, 1, 19)),
                ('2010-12-29', '%Y-%m-%d', datetime(2010, 12, 29)),
                ('2012-12', '%Y-%m', datetime(2012, 12, 1)),
                ('2011.01', '%Y.%m', datetime(2011, 1, 1)),
                ('2014-28', '%Y-%W', datetime(2014, 7, 14)),
                ('2014-28', '%Y-%U', datetime(2014, 7, 14)),
                ('2010.12.29.12', '%Y.%m.%d.%H', datetime(2010, 12, 29, 12)),
                ('2009101112136', '%Y%m%d%H%M%S', datetime(2009, 10, 11, 12, 13, 6)),
                ('2016-03-30t16', '%Y-%m-%dt%H', datetime(2016, 3, 30, 16, 0)),
                # ISO weeks
                # In 2014 ISO weeks were one week more than Greg weeks
                ('2014-42', '%Y-%W', datetime(2014, 10, 20)),
                ('2014-42', '%G-%V', datetime(2014, 10, 13)),
                ('2014-43', '%G-%V', datetime(2014, 10, 20)),
                #
                ('2008-52', '%G-%V', datetime(2008, 12, 22)),
                ('2008-52', '%Y-%W', datetime(2008, 12, 29)),
                ('2009-01', '%Y-%W', datetime(2009, 1, 5)),
                ('2009-01', '%G-%V', datetime(2008, 12, 29)),
                # The case when both ISO and Greg are same week number
                ('2017-16', '%Y-%W', datetime(2017, 4, 17)),
                ('2017-16', '%G-%V', datetime(2017, 4, 17)),
                # Weeks were leading 0 is needed for week number
                ('2017-02', '%Y-%W', datetime(2017, 1, 9)),
                ('2017-02', '%G-%V', datetime(2017, 1, 9)),
                ('2010-01', '%G-%V', datetime(2010, 1, 4)),
                ('2010-01', '%Y-%W', datetime(2010, 1, 4)),
                # In Greg week 53 for year 2009 doesn't exist, it converts to week 1 of next year.
                ('2009-53', '%Y-%W', datetime(2010, 1, 4)),
                ('2009-53', '%G-%V', datetime(2009, 12, 28)),
            ]:
            self.assertEqual(dt, get_datetime(text, datestring))

class TestGetDateRegex(TestCase):
    def test_non_escaped(self):
        self.assertEqual(
            '\\d{4}\\-\\d{2}\\-\\d{2}t\\d{2}',
            get_date_regex('%Y-%m-%dt%H')
        )
class TestFixEpoch(TestCase):
    def test_fix_epoch(self):
        for long_epoch, epoch in [
                (1459287636, 1459287636),
                (14592876369, 14592876),
                (145928763699, 145928763),
                (1459287636999, 1459287636),
                (1459287636000000, 1459287636),
                (145928763600000000, 1459287636),
                (145928763600000001, 1459287636),
                (1459287636123456789, 1459287636),
            ]:
            self.assertEqual(epoch, fix_epoch(long_epoch))
    def test_fix_epoch_raise(self):
            self.assertRaises(ValueError, fix_epoch, None)

class TestGetPointOfReference(TestCase):
    def test_get_point_of_reference(self):
        epoch = 1459288037
        for unit, result in [
                ('seconds', epoch-1),
                ('minutes', epoch-60),
                ('hours', epoch-3600),
                ('days', epoch-86400),
                ('weeks', epoch-(86400*7)),
                ('months', epoch-(86400*30)),
                ('years', epoch-(86400*365)),
            ]:
            self.assertEqual(result, get_point_of_reference(unit, 1, epoch))
    def test_get_por_raise(self):
        self.assertRaises(ValueError, get_point_of_reference, 'invalid', 1)
    def test_por_null(self):
        self.assertLessEqual(get_point_of_reference('days', 1), datetime_to_epoch(datetime.now()))

class TestDateRange(TestCase):
    def test_bad_unit(self):
        self.assertRaises(ConfigurationError, date_range, 'invalid', 1, 1)
    def test_bad_range(self):
        self.assertRaises(ConfigurationError, date_range, 'hours', 1, -1)
    def test_now_live(self):
        unit = 'hours'
        start, end = date_range(unit, -1, -1)
        self.assertLessEqual(start, end)
    def test_hour_ranges(self):
        unit = 'hours'
        date1 = make_epoch(2017, 4, 3, 21, 59, 59)
        date2 = make_epoch(2017, 4, 4, 0, 59, 59)
        for tuples in [
                ((-1, -1), (make_epoch(2017, 4, 3, 21, 0, 0), date1)),
                ((-3, -1), (make_epoch(2017, 4, 3, 19, 0, 0), date1)),
                ((0, 2), (make_epoch(2017, 4, 3, 22, 0, 0), date2)),
                ((-1, 2), (make_epoch(2017, 4, 3, 21, 0, 0), date2)),
            ]:
            range_from, range_to = tuples[0]
            start, end = tuples[1]
            self.assertEqual((start, end), date_range(unit, range_from, range_to, epoch=EPOCH))
    def test_day_ranges(self):
        unit = 'days'
        date1 = make_epoch(2017, 4, 2, 23, 59, 59)
        date2 = make_epoch(2017, 4, 5, 23, 59, 59)
        for tuples in [
                ((-1, -1), (make_epoch(2017, 4, 2, 0, 0, 0), date1)),
                ((-3, -1), (make_epoch(2017, 3, 31, 0, 0, 0), date1)),
                ((0, 2), (make_epoch(2017, 4, 3, 0, 0, 0), date2)),
                ((-1, 2), (make_epoch(2017, 4, 2, 0, 0, 0), date2)),
            ]:
            range_from, range_to = tuples[0]
            start, end = tuples[1]
            self.assertEqual((start, end), date_range(unit, range_from, range_to, epoch=EPOCH))
    def test_week_ranges(self):
        unit = 'weeks'
        date1 = make_epoch(2017, 4, 1, 23, 59, 59)
        date2 = make_epoch(2017, 4, 22, 23, 59, 59)
        for tuples in [
                ((-1, -1), (make_epoch(2017, 3, 26, 0, 0, 0), date1)),
                ((-3, -1), (make_epoch(2017, 3, 12, 0, 0, 0), date1)),
                ((0, 2), (make_epoch(2017, 4, 2, 0, 0, 0), date2)),
                ((-1, 2), (make_epoch(2017, 3, 26, 0, 0, 0), date2)),
            ]:
            range_from, range_to = tuples[0]
            start, end = tuples[1]
            self.assertEqual((start, end), date_range(unit, range_from, range_to, epoch=EPOCH))
    def test_iso_week_ranges(self):
        unit = 'weeks'
        date1 = make_epoch(2017, 4, 2, 23, 59, 59)
        date2 = make_epoch(2017, 4, 23, 23, 59, 59)
        for tuples in [
                ((-1, -1), (make_epoch(2017, 3, 27, 0, 0, 0), date1)),
                ((-3, -1), (make_epoch(2017, 3, 13, 0, 0, 0), date1)),
                ((0, 2), (make_epoch(2017, 4, 3, 0, 0, 0), date2)),
                ((-1, 2), (make_epoch(2017, 3, 27, 0, 0, 0), date2)),
            ]:
            range_from, range_to = tuples[0]
            start, end = tuples[1]
            self.assertEqual(
                (start, end),
                date_range(unit, range_from, range_to, epoch=EPOCH, week_starts_on='monday')
            )
    def test_month_ranges(self):
        unit = 'months'
        date1 = make_epoch(2017, 3, 31, 23, 59, 59)
        date2 = make_epoch(2018, 2, 28, 23, 59, 59)
        date3 = make_epoch(2017, 6, 30, 23, 59, 59)
        for tuples in [
                ((-1, -1), (make_epoch(2017, 3, 1, 0, 0, 0), date1)),
                ((-4, -1), (make_epoch(2016, 12, 1, 0, 0, 0), date1)),
                ((7, 10), (make_epoch(2017, 11, 1, 0, 0, 0), date2)),
                ((9, 10), (make_epoch(2018, 1, 1, 0, 0, 0), date2)),
                ((-1, 2), (make_epoch(2017, 3, 1, 0, 0, 0), date3)),
            ]:
            range_from, range_to = tuples[0]
            start, end = tuples[1]
            self.assertEqual((start, end), date_range(unit, range_from, range_to, epoch=EPOCH))
    def test_year_ranges(self):
        unit = 'years'
        date1 = make_epoch(2016, 12, 31, 23, 59, 59)
        date2 = make_epoch(2019, 12, 31, 23, 59, 59)
        for tuples in [
                ((-1, -1), (make_epoch(2016, 1, 1, 0, 0, 0), date1)),
                ((-3, -1), (make_epoch(2014, 1, 1, 0, 0, 0), date1)),
                ((0, 2), (make_epoch(2017, 1, 1, 0, 0, 0), date2)),
                ((-1, 2), (make_epoch(2016, 1, 1, 0, 0, 0), date2)),
            ]:
            range_from, range_to = tuples[0]
            start, end = tuples[1]
            self.assertEqual((start, end), date_range(unit, range_from, range_to, epoch=EPOCH))

class TestAbsoluteDateRange(TestCase):
    def test_bad_unit(self):
        unit = 'invalid'
        date_from = '2017.01'
        date_from_format = '%Y.%m'
        date_to = '2017.01'
        date_to_format = '%Y.%m'
        self.assertRaises(
            ConfigurationError,
            absolute_date_range, unit, date_from, date_to, date_from_format, date_to_format
        )
    def test_bad_formats(self):
        unit = 'days'
        self.assertRaises(
            ConfigurationError,
            absolute_date_range, unit, 'meh', 'meh', None, 'meh'
        )
        self.assertRaises(
            ConfigurationError,
            absolute_date_range, unit, 'meh', 'meh', 'meh', None
        )
    def test_bad_dates(self):
        unit = 'weeks'
        date_from_format = '%Y.%m'
        date_to_format = '%Y.%m'
        self.assertRaises(
            ConfigurationError,
            absolute_date_range, unit, 'meh', '2017.01', date_from_format, date_to_format
        )
        self.assertRaises(
            ConfigurationError,
            absolute_date_range, unit, '2017.01', 'meh', date_from_format, date_to_format
        )
    def test_absolute_dates(self):
        tuples = [
            ('seconds', '2017-01-01T00:00:00', '%Y-%m-%dT%H:%M:%S', '2017-01-01T12:34:56',
             '%Y-%m-%dT%H:%M:%S', make_epoch(2017, 1, 1, 0, 0, 0),
             make_epoch(2017, 1, 1, 12, 34, 56)
            ),
            ('days', '2017.01.01', '%Y.%m.%d', '2017.01.01', '%Y.%m.%d',
             make_epoch(2017, 1, 1, 0, 0, 0), make_epoch(2017, 1, 1, 23, 59, 59)),
            ('days', '2016.12.31', '%Y.%m.%d', '2017.01.01', '%Y.%m.%d',
             make_epoch(2016, 12, 31, 0, 0, 0), make_epoch(2017, 1, 1, 23, 59, 59)),
            ('weeks', '2017-01', '%Y-%U', '2017-01', '%Y-%U',
             make_epoch(2017, 1, 2, 0, 0, 0), make_epoch(2017, 1, 8, 23, 59, 59)),
            ('weeks', '2017-01', '%Y-%U', '2017-04', '%Y-%U',
             make_epoch(2017, 1, 2, 0, 0, 0), make_epoch(2017, 1, 29, 23, 59, 59)),
            ('weeks', '2014-01', '%G-%V', '2014-01', '%G-%V',
             make_epoch(2013, 12, 30, 0, 0, 0), make_epoch(2014, 1, 5, 23, 59, 59)),
            ('weeks', '2014-01', '%G-%V', '2014-04', '%G-%V',
             make_epoch(2013, 12, 30, 0, 0, 0), make_epoch(2014, 1, 26, 23, 59, 59)),
            ('months', '2017.01', '%Y.%m', '2017.01', '%Y.%m',
             make_epoch(2017, 1, 1, 0, 0, 0), make_epoch(2017, 1, 31, 23, 59, 59)),
            ('months', '2016.11', '%Y.%m', '2016.12', '%Y.%m',
             make_epoch(2016, 11, 1, 0, 0, 0), make_epoch(2016, 12, 31, 23, 59, 59)),
            ('years', '2017', '%Y', '2017', '%Y',
             make_epoch(2017, 1, 1, 0, 0, 0), make_epoch(2017, 12, 31, 23, 59, 59)),
            ('years', '2016', '%Y', '2017', '%Y',
             make_epoch(2016, 1, 1, 0, 0, 0), make_epoch(2017, 12, 31, 23, 59, 59)),
        ]
        for values in tuples:
            unit = values[0]
            date_from = values[1]
            date_from_format = values[2]
            date_to = values[3]
            date_to_format = values[4]
            start = values[5]
            end = values[6]
            self.assertEqual(
                (start, end),
                absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format)
            )

class TestIsDateMath(TestCase):
    def test_positive(self):
        data = '<encapsulated>'
        self.assertTrue(isdatemath(data))
    def test_negative(self):
        data = 'not_encapsulated'
        self.assertFalse(isdatemath(data))
    def test_raises(self):
        data = '<badly_encapsulated'
        self.assertRaises(ConfigurationError, isdatemath, data)

class TestGetDateMath(TestCase):
    def test_success(self):
        client = Mock()
        datemath = u'{hasthemath}'
        psuedo_random = u'not_random_at_all'
        expected = u'curator_get_datemath_function_' + psuedo_random + u'-hasthemath'
        client.indices.get.side_effect = (
            NotFoundError(404, 'simulated error', {u'error':{u'index':expected}}))
        self.assertEqual('hasthemath', get_datemath(client, datemath, psuedo_random))
    def test_failure(self):
        client = Mock()
        datemath = u'{hasthemath}'
        client.indices.get.side_effect = TypeError
        self.assertRaises(ConfigurationError, get_datemath, client, datemath)

class TestUnitInName(TestCase):
    def test_no_pattern(self):
        self.assertIsNone(get_unit_count_from_name('sample', None))
    def test_with_match(self):
        index_name = 'index-2017.01.01-1'
        pattern = r'^index-\d{4}\.\d{2}\.\d{2}-(\d)$'
        self.assertEqual(1, get_unit_count_from_name(index_name, re.compile(pattern)))
    def test_without_match(self):
        index_name = 'index-2017.01.01'
        pattern = r'^index-\d{4}\.\d{2}\.\d{2}-(\d)$'
        self.assertIsNone(get_unit_count_from_name(index_name, re.compile(pattern)))
    def test_non_integer_match(self):
        index_name = 'index-2017.01.01-foo'
        pattern = r'^index-\d{4}\.\d{2}\.\d{2}-(.+)$'
        self.assertIsNone(get_unit_count_from_name(index_name, re.compile(pattern)))

class TestParseDatePattern(TestCase):
    def test_date_math(self):
        name = '<foo>'
        self.assertEquals(name, parse_date_pattern(name))
    def test_strfstring(self):
        year = str(datetime.now().year)
        name = '%Y'
        self.assertEquals(year, parse_date_pattern(name))

class TestTimestringSearch(TestCase):
    def test_epoch_value(self):
        tstring = TimestringSearch('%Y.%m.%d')
        self.assertEqual(
            make_epoch(2017, 1, 1, 0, 0, 0),
            tstring.get_epoch('index-2017.01.01')
        )
