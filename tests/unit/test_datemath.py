"""Test datemath functions"""
# pylint: disable=C0103,C0111
from datetime import datetime, timedelta
from unittest import TestCase
from mock import Mock
import elasticsearch
from curator_api.helpers.datemath import (
    fix_epoch, get_date_regex, get_datetime, get_point_of_reference)
from . import testvars as testvars

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

# class TestDateRange(TestCase):
#     def test_bad_unit(self):
#         self.assertRaises(curator.ConfigurationError,
#             curator.date_range, 'invalid', 1, 1
#         )
#     def test_bad_range(self):
#         self.assertRaises(curator.ConfigurationError,
#             curator.date_range, 'hours', 1, -1
#         )
#     def test_hours_single(self):
#         unit = 'hours'
#         range_from = -1
#         range_to = -1
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2017,  4,  3, 21,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  4,  3, 21, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))
#     def test_hours_past_range(self):
#         unit = 'hours'
#         range_from = -3
#         range_to = -1
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2017,  4,  3, 19,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  4,  3, 21, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))
#     def test_hours_future_range(self):
#         unit = 'hours'
#         range_from = 0
#         range_to = 2
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2017,  4,  3, 22,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  4,  4, 00, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))
#     def test_hours_span_range(self):
#         unit = 'hours'
#         range_from = -1
#         range_to = 2
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2017,  4,  3, 21,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  4,  4, 00, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))
#     def test_days_single(self):
#         unit = 'days'
#         range_from = -1
#         range_to = -1
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2017,  4,  2,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  4,  2, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))
#     def test_days_past_range(self):
#         unit = 'days'
#         range_from = -3
#         range_to = -1
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2017,  3, 31,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  4,  2, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))
#     def test_days_future_range(self):
#         unit = 'days'
#         range_from = 0
#         range_to = 2
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2017,  4,  3,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  4,  5, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))
#     def test_days_span_range(self):
#         unit = 'days'
#         range_from = -1
#         range_to = 2
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2017,  4,  2,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  4,  5, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))
#     def test_weeks_single(self):
#         unit = 'weeks'
#         range_from = -1
#         range_to = -1
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2017,  3, 26,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  4,  1, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))
#     def test_weeks_past_range(self):
#         unit = 'weeks'
#         range_from = -3
#         range_to = -1
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2017,  3, 12,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  4,  1, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))
#     def test_weeks_future_range(self):
#         unit = 'weeks'
#         range_from = 0
#         range_to = 2
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2017,  4,  2, 00,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  4, 22, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))
#     def test_weeks_span_range(self):
#         unit = 'weeks'
#         range_from = -1
#         range_to = 2
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2017,  3, 26,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  4, 22, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))
#     def test_weeks_single_iso(self):
#         unit = 'weeks'
#         range_from = -1
#         range_to = -1
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2017,  3, 27,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  4,  2, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch,
#                 week_starts_on='monday')
#         )
#     def test_weeks_past_range_iso(self):
#         unit = 'weeks'
#         range_from = -3
#         range_to = -1
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2017,  3, 13,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  4,  2, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch,
#                 week_starts_on='monday')
#         )
#     def test_weeks_future_range_iso(self):
#         unit = 'weeks'
#         range_from = 0
#         range_to = 2
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2017,  4,  3,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  4, 23, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch,
#                 week_starts_on='monday')
#         )
#     def test_weeks_span_range_iso(self):
#         unit = 'weeks'
#         range_from = -1
#         range_to = 2
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2017,  3, 27,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  4, 23, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch,
#                 week_starts_on='monday')
#         )
#     def test_months_single(self):
#         unit = 'months'
#         range_from = -1
#         range_to = -1
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2017,  3,  1,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  3, 31, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))
#     def test_months_past_range(self):
#         unit = 'months'
#         range_from = -4
#         range_to = -1
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2016, 12,  1,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  3, 31, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))
#     def test_months_future_range(self):
#         unit = 'months'
#         range_from = 7
#         range_to = 10
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2017, 11,  1,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2018,  2, 28, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))
#     def test_months_super_future_range(self):
#         unit = 'months'
#         range_from = 9
#         range_to = 10
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2018,  1,  1,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2018,  2, 28, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))
#     def test_months_span_range(self):
#         unit = 'months'
#         range_from = -1
#         range_to = 2
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2017,  3,  1,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  6, 30, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))
#     def test_years_single(self):
#         unit = 'years'
#         range_from = -1
#         range_to = -1
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2016,  1,  1,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2016, 12, 31, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))
#     def test_years_past_range(self):
#         unit = 'years'
#         range_from = -3
#         range_to = -1
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2014,  1,  1,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2016, 12, 31, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))
#     def test_years_future_range(self):
#         unit = 'years'
#         range_from = 0
#         range_to = 2
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2017,  1,  1,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2019, 12, 31, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))
#     def test_years_span_range(self):
#         unit = 'years'
#         range_from = -1
#         range_to = 2
#         epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
#         start = curator.datetime_to_epoch(datetime(2016,  1,  1,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2019, 12, 31, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.date_range(unit, range_from, range_to, epoch=epoch))

# class TestAbsoluteDateRange(TestCase):
#     def test_bad_unit(self):
#         unit = 'invalid'
#         date_from = '2017.01'
#         date_from_format = '%Y.%m'
#         date_to = '2017.01'
#         date_to_format = '%Y.%m'
#         self.assertRaises(
#             curator.ConfigurationError,
#             curator.absolute_date_range, unit,
#             date_from, date_to, date_from_format, date_to_format
#         )
#     def test_bad_formats(self):
#         unit = 'days'
#         self.assertRaises(
#             curator.ConfigurationError,
#             curator.absolute_date_range, unit, 'meh', 'meh', None, 'meh'
#         )
#         self.assertRaises(
#             curator.ConfigurationError,
#             curator.absolute_date_range, unit, 'meh', 'meh', 'meh', None
#         )
#     def test_bad_dates(self):
#         unit = 'weeks'
#         date_from_format = '%Y.%m'
#         date_to_format = '%Y.%m'
#         self.assertRaises(
#             curator.ConfigurationError,
#             curator.absolute_date_range, unit, 'meh', '2017.01', date_from_format, date_to_format
#         )
#         self.assertRaises(
#             curator.ConfigurationError,
#             curator.absolute_date_range, unit, '2017.01', 'meh', date_from_format, date_to_format
#         )
#     def test_single_month(self):
#         unit = 'months'
#         date_from = '2017.01'
#         date_from_format = '%Y.%m'
#         date_to = '2017.01'
#         date_to_format = '%Y.%m'
#         start = curator.datetime_to_epoch(datetime(2017,  1,  1,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  1, 31, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.absolute_date_range(
#                 unit, date_from, date_to, date_from_format, date_to_format))
#     def test_multiple_month(self):
#         unit = 'months'
#         date_from = '2016.11'
#         date_from_format = '%Y.%m'
#         date_to = '2016.12'
#         date_to_format = '%Y.%m'
#         start = curator.datetime_to_epoch(datetime(2016, 11,  1,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2016, 12, 31, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.absolute_date_range(
#                 unit, date_from, date_to, date_from_format, date_to_format))
#     def test_single_year(self):
#         unit = 'years'
#         date_from = '2017'
#         date_from_format = '%Y'
#         date_to = '2017'
#         date_to_format = '%Y'
#         start = curator.datetime_to_epoch(datetime(2017,  1,  1,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017, 12, 31, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.absolute_date_range(
#                 unit, date_from, date_to, date_from_format, date_to_format))
#     def test_multiple_year(self):
#         unit = 'years'
#         date_from = '2016'
#         date_from_format = '%Y'
#         date_to = '2017'
#         date_to_format = '%Y'
#         start = curator.datetime_to_epoch(datetime(2016,  1,  1,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017, 12, 31, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.absolute_date_range(
#                 unit, date_from, date_to, date_from_format, date_to_format))
#     def test_single_week_UW(self):
#         unit = 'weeks'
#         date_from = '2017-01'
#         date_from_format = '%Y-%U'
#         date_to = '2017-01'
#         date_to_format = '%Y-%U'
#         start = curator.datetime_to_epoch(datetime(2017,  1,  2,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  1,  8, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.absolute_date_range(
#                 unit, date_from, date_to, date_from_format, date_to_format))
#     def test_multiple_weeks_UW(self):
#         unit = 'weeks'
#         date_from = '2017-01'
#         date_from_format = '%Y-%U'
#         date_to = '2017-04'
#         date_to_format = '%Y-%U'
#         start = curator.datetime_to_epoch(datetime(2017,  1,   2,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  1,  29, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.absolute_date_range(
#                 unit, date_from, date_to, date_from_format, date_to_format))
#     def test_single_week_ISO(self):
#         unit = 'weeks'
#         date_from = '2014-01'
#         date_from_format = '%G-%V'
#         date_to = '2014-01'
#         date_to_format = '%G-%V'
#         start = curator.datetime_to_epoch(datetime(2013, 12, 30,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2014,  1,  5, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.absolute_date_range(
#                 unit, date_from, date_to, date_from_format, date_to_format))
#     def test_multiple_weeks_ISO(self):
#         unit = 'weeks'
#         date_from = '2014-01'
#         date_from_format = '%G-%V'
#         date_to = '2014-04'
#         date_to_format = '%G-%V'
#         start = curator.datetime_to_epoch(datetime(2013, 12, 30,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2014,  1, 26, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.absolute_date_range(
#                 unit, date_from, date_to, date_from_format, date_to_format))
#     def test_single_day(self):
#         unit = 'days'
#         date_from = '2017.01.01'
#         date_from_format = '%Y.%m.%d'
#         date_to = '2017.01.01'
#         date_to_format = '%Y.%m.%d'
#         start = curator.datetime_to_epoch(datetime(2017,  1,  1,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  1,  1, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.absolute_date_range(
#                 unit, date_from, date_to, date_from_format, date_to_format))
#     def test_multiple_days(self):
#         unit = 'days'
#         date_from = '2016.12.31'
#         date_from_format = '%Y.%m.%d'
#         date_to = '2017.01.01'
#         date_to_format = '%Y.%m.%d'
#         start = curator.datetime_to_epoch(datetime(2016, 12, 31,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  1,  1, 23, 59, 59))
#         self.assertEqual((start,end),
#             curator.absolute_date_range(
#                 unit, date_from, date_to, date_from_format, date_to_format))
#     def test_ISO8601(self):
#         unit = 'seconds'
#         date_from = '2017-01-01T00:00:00'
#         date_from_format = '%Y-%m-%dT%H:%M:%S'
#         date_to = '2017-01-01T12:34:56'
#         date_to_format = '%Y-%m-%dT%H:%M:%S'
#         start = curator.datetime_to_epoch(datetime(2017,  1,  1,  0,  0,  0))
#         end   = curator.datetime_to_epoch(datetime(2017,  1,  1, 12, 34, 56))
#         self.assertEqual((start,end),
#             curator.absolute_date_range(
#                 unit, date_from, date_to, date_from_format, date_to_format))


# class TestIsDateMath(TestCase):
#     def test_positive(self):
#         data = '<encapsulated>'
#         self.assertTrue(curator.isdatemath(data))
#     def test_negative(self):
#         data = 'not_encapsulated'
#         self.assertFalse(curator.isdatemath(data))
#     def test_raises(self):
#         data = '<badly_encapsulated'
#         self.assertRaises(curator.ConfigurationError, curator.isdatemath, data)

# class TestGetDateMath(TestCase):
#     def test_success(self):
#         client = Mock()
#         datemath = u'{hasthemath}'
#         psuedo_random = u'not_random_at_all'
#         expected = u'curator_get_datemath_function_' + psuedo_random + u'-hasthemath'
#         client.indices.get.side_effect = (
#             elasticsearch.NotFoundError(
#                 404, "simulated error", {u'error':{u'index':expected}})
#         )
#         self.assertEqual('hasthemath', curator.get_datemath(client, datemath, psuedo_random))
#     def test_failure(self):
#         client = Mock()
#         datemath = u'{hasthemath}'
#         client.indices.get.side_effect = (
#             elasticsearch.NotFoundError(
#                 404, "simulated error", {u'error':{u'index':'failure'}})
#         )
#         self.assertRaises(curator.ConfigurationError, curator.get_datemath, client, datemath)
