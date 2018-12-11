"""Datemath Module"""
import logging
import random
import re
import string
from datetime import timedelta, datetime, date
from time import time
from elasticsearch.exceptions import NotFoundError
from curator_api.exceptions import ConfigurationError, FailedExecution
from curator_api.defaults.settings import date_regex


def get_date_regex(timestring):
    """
    Return a regex string based on a provided strftime timestring.

    :arg timestring: An strftime pattern
    :rtype: str
    """
    logger = logging.getLogger(__name__)
    prev, curr, regex = '', '', ''
    for curr in timestring:
        if curr == '%':
            pass
        elif curr in date_regex() and prev == '%':
            regex += r'\d{' + date_regex()[curr] + '}'
        elif curr in ['.', '-']:
            regex += "\\" + curr
        else:
            regex += curr
        prev = curr
    logger.debug("regex = {0}".format(regex))
    return regex

def get_datetime(index_timestamp, timestring):
    """
    Return the datetime extracted from the index name, which is the index
    creation time.

    :arg index_timestamp: The timestamp extracted from an index name
    :arg timestring: An strftime pattern
    :rtype: :py:class:`datetime.datetime`
    """
    # Compensate for week of year by appending '%w' to the timestring
    # and '1' (Monday) to index_timestamp
    iso_week_number = False
    if '%W' in timestring or '%U' in timestring or '%V' in timestring:
        timestring += '%w'
        index_timestamp += '1'
        if '%V' in timestring and '%G' in timestring:
            iso_week_number = True
            # Fake as so we read Greg format instead. We will process it later
            timestring = timestring.replace("%G", "%Y").replace("%V", "%W")
    elif '%m' in timestring:
        if not '%d' in timestring:
            timestring += '%d'
            index_timestamp += '1'

    date = datetime.strptime(index_timestamp, timestring)

    # Handle ISO time string
    if iso_week_number:
        date = _handle_iso_week_number(date, timestring, index_timestamp)

    return date

def fix_epoch(epoch):
    """
    Fix value of `epoch` to be epoch, which should be 10 or fewer digits long.

    :arg epoch: An epoch timestamp, in epoch + milliseconds, or microsecond, or
        even nanoseconds.
    :rtype: int
    """
    try:
        # No decimals allowed
        epoch = int(epoch)
    except Exception:
        raise ValueError('Invalid epoch received, unable to convert {0} to int'.format(epoch))

    # If we're still using this script past January, 2038, we have bigger
    # problems than my hacky math here...
    if len(str(epoch)) <= 10:
        return epoch
    elif len(str(epoch)) > 10 and len(str(epoch)) <= 13:
        return int(epoch/1000)
    else:
        orders_of_magnitude = len(str(epoch)) - 10
        powers_of_ten = 10**orders_of_magnitude
        epoch = int(epoch/powers_of_ten)
    return epoch

def _handle_iso_week_number(date, timestring, index_timestamp):
    date_iso = date.isocalendar()
    iso_week_str = "{Y:04d}{W:02d}".format(Y=date_iso[0], W=date_iso[1])
    greg_week_str = datetime.strftime(date, "%Y%W")

    # Edge case 1: ISO week number is bigger than Greg week number.
    # Ex: year 2014, all ISO week numbers were 1 more than in Greg.
    # Edge case 2: 2010-01-01 in ISO: 2009.W53, in Greg: 2010.W00
    # For Greg converting 2009.W53 gives 2010-01-04, converting back
    # to same timestring gives: 2010.W01.
    if (iso_week_str > greg_week_str or datetime.strftime(date, timestring) != index_timestamp):

        # Remove one week in this case
        date = date - timedelta(days=7)
    return date

def datetime_to_epoch(mydate):
    """Convert a datetime object to epoch time"""
    # I would have used `total_seconds`, but apparently that's new
    # to Python 2.7+, and due to so many people still using
    # RHEL/CentOS 6, I need this to support Python 2.6.
    tdelta = (mydate - datetime(1970, 1, 1))
    return tdelta.seconds + tdelta.days * 24 * 3600

class TimestringSearch(object):
    """
    An object to allow repetitive search against a string, `searchme`, without
    having to repeatedly recreate the regex.

    :arg timestring: An strftime pattern
    """
    def __init__(self, timestring):
        regex = r'(?P<date>{0})'.format(get_date_regex(timestring))
        self.pattern = re.compile(regex)
        self.timestring = timestring
    def get_epoch(self, searchme):
        """
        Return the epoch timestamp extracted from the `timestring` appearing in
        `searchme`.

        :arg searchme: A string to be searched for a date pattern that matches
            `timestring`
        :rtype: int
        """
        match = self.pattern.search(searchme)
        if match:
            if match.group("date"):
                timestamp = match.group("date")
                return datetime_to_epoch(
                    get_datetime(timestamp, self.timestring)
                )
                # # I would have used `total_seconds`, but apparently that's new
                # # to Python 2.7+, and due to so many people still using
                # # RHEL/CentOS 6, I need this to support Python 2.6.
                # tdelta = (
                #     get_datetime(timestamp, self.timestring) -
                #     datetime(1970,1,1)
                # )
                # return tdelta.seconds + tdelta.days * 24 * 3600

def get_point_of_reference(unit, count, epoch=None):
    """
    Get a point-of-reference timestamp in epoch + milliseconds by deriving
    from a `unit` and a `count`, and an optional reference timestamp, `epoch`

    :arg unit: One of ``seconds``, ``minutes``, ``hours``, ``days``, ``weeks``,
        ``months``, or ``years``.
    :arg unit_count: The number of ``units``. ``unit_count`` * ``unit`` will
        be calculated out to the relative number of seconds.
    :arg epoch: An epoch timestamp used in conjunction with ``unit`` and
        ``unit_count`` to establish a point of reference for calculations.
    :rtype: int
    """
    if unit == 'seconds':
        multiplier = 1
    elif unit == 'minutes':
        multiplier = 60
    elif unit == 'hours':
        multiplier = 3600
    elif unit == 'days':
        multiplier = 3600*24
    elif unit == 'weeks':
        multiplier = 3600*24*7
    elif unit == 'months':
        multiplier = 3600*24*30
    elif unit == 'years':
        multiplier = 3600*24*365
    else:
        raise ValueError('Invalid unit: {0}.'.format(unit))
    # Use this moment as a reference point, if one is not provided.
    if not epoch:
        epoch = time()
    epoch = fix_epoch(epoch)
    return epoch - multiplier * count

def get_unit_count_from_name(index_name, pattern):
    """Extract a unit_count from an index_name"""
    logger = logging.getLogger(__name__)
    if pattern is None:
        return None
    match = pattern.search(index_name)
    if match:
        try:
            return int(match.group(1))
        except Exception as err:
            logger.debug('Unable to convert value to integer: {0}'.format(err))
            return None
    else:
        return None

def date_range(unit, range_from, range_to, epoch=None, week_starts_on='sunday'):
    """
    Get the epoch start time and end time of a range of ``unit``s, reckoning the
    start of the week (if that's the selected unit) based on ``week_starts_on``,
    which can be either ``sunday`` or ``monday``.

    :arg unit: One of ``hours``, ``days``, ``weeks``, ``months``, or ``years``.
    :arg range_from: How many ``unit`` (s) in the past/future is the origin?
    :arg range_to: How many ``unit`` (s) in the past/future is the end point?
    :arg epoch: An epoch timestamp used to establish a point of reference for
        calculations.
    :arg week_starts_on: Either ``sunday`` or ``monday``. Default is ``sunday``
    :rtype: tuple
    """
    logger = logging.getLogger(__name__)
    acceptable_units = ['hours', 'days', 'weeks', 'months', 'years']
    if unit not in acceptable_units:
        raise ConfigurationError(
            '"unit" must be one of: {0}'.format(acceptable_units))
    if not range_to >= range_from:
        raise ConfigurationError(
            '"range_to" must be greater than or equal to "range_from"')
    if not epoch:
        epoch = time()
    epoch = fix_epoch(epoch)
    raw_por = datetime.utcfromtimestamp(epoch)
    logger.debug('Raw point of reference = {0}'.format(raw_por))
    # Reverse the polarity, because -1 as last week makes sense when read by
    # humans, but datetime timedelta math makes -1 in the future.
    origin = range_from * -1
    # These if statements help get the start date or start_delta
    if unit == 'hours':
        por = datetime(raw_por.year, raw_por.month, raw_por.day, raw_por.hour, 0, 0)
        start_delta = timedelta(hours=origin)
    if unit == 'days':
        por = datetime(raw_por.year, raw_por.month, raw_por.day, 0, 0, 0)
        start_delta = timedelta(days=origin)
    if unit == 'weeks':
        por = datetime(raw_por.year, raw_por.month, raw_por.day, 0, 0, 0)
        sunday = False
        if week_starts_on.lower() == 'sunday':
            sunday = True
        weekday = por.weekday()
        # Compensate for ISO week starting on Monday by default
        if sunday:
            weekday += 1
        logger.debug('Weekday = {0}'.format(weekday))
        start_delta = timedelta(days=weekday, weeks=origin)
    if unit == 'months':
        por = datetime(raw_por.year, raw_por.month, 1, 0, 0, 0)
        year = raw_por.year
        month = raw_por.month
        if origin > 0:
            for _ in range(0, origin):
                if month == 1:
                    year -= 1
                    month = 12
                else:
                    month -= 1
        else:
            for _ in range(origin, 0):
                if month == 12:
                    year += 1
                    month = 1
                else:
                    month += 1
        start_date = datetime(year, month, 1, 0, 0, 0)
    if unit == 'years':
        por = datetime(raw_por.year, 1, 1, 0, 0, 0)
        start_date = datetime(raw_por.year - origin, 1, 1, 0, 0, 0)
    if unit not in ['months', 'years']:
        start_date = por - start_delta
    # By this point, we know our start date and can convert it to epoch time
    start_epoch = datetime_to_epoch(start_date)
    logger.debug('Start ISO8601 = {0}'.format(
        datetime.utcfromtimestamp(start_epoch).isoformat()))
    # This is the number of units we need to consider.
    count = (range_to - range_from) + 1
    # We have to iterate to one more month, and then subtract a second to get
    # the last day of the correct month
    if unit == 'months':
        month = start_date.month
        year = start_date.year
        for _ in range(0, count):
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1
        end_date = datetime(year, month, 1, 0, 0, 0)
        end_epoch = datetime_to_epoch(end_date) - 1
    # Similarly, with years, we need to get the last moment of the year
    elif unit == 'years':
        end_date = datetime((raw_por.year - origin) + count, 1, 1, 0, 0, 0)
        end_epoch = datetime_to_epoch(end_date) - 1
    # It's not months or years, which have inconsistent reckoning...
    else:
        # This lets us use an existing method to simply add unit * count seconds
        # to get hours, days, or weeks, as they don't change
        end_epoch = get_point_of_reference(
            unit, count * -1, epoch=start_epoch) -1
    logger.debug('End ISO8601 = {0}'.format(
        datetime.utcfromtimestamp(end_epoch).isoformat()))
    return (start_epoch, end_epoch)

def absolute_date_range(
        unit, date_from, date_to,
        date_from_format=None, date_to_format=None
    ):
    """
    Get the epoch start time and end time of a range of ``unit``s, reckoning the
    start of the week (if that's the selected unit) based on ``week_starts_on``,
    which can be either ``sunday`` or ``monday``.

    :arg unit: One of ``hours``, ``days``, ``weeks``, ``months``, or ``years``.
    :arg date_from: The simplified date for the start of the range
    :arg date_to: The simplified date for the end of the range.  If this value
        is the same as ``date_from``, the full value of ``unit`` will be
        extrapolated for the range.  For example, if ``unit`` is ``months``,
        and ``date_from`` and ``date_to`` are both ``2017.01``, then the entire
        month of January 2017 will be the absolute date range.
    :arg date_from_format: The strftime string used to parse ``date_from``
    :arg date_to_format: The strftime string used to parse ``date_to``
    :rtype: tuple
    """
    logger = logging.getLogger(__name__)
    acceptable_units = ['seconds', 'minutes', 'hours', 'days', 'weeks', 'months', 'years']
    if unit not in acceptable_units:
        raise ConfigurationError(
            '"unit" must be one of: {0}'.format(acceptable_units))
    if not date_from_format or not date_to_format:
        raise ConfigurationError('Must provide "date_from_format" and "date_to_format"')
    try:
        start_epoch = datetime_to_epoch(get_datetime(date_from, date_from_format))
        logger.debug('Start ISO8601 = {0}'.format(
            datetime.utcfromtimestamp(start_epoch).isoformat()))
    except Exception as err:
        raise ConfigurationError(
            'Unable to parse "date_from" {0} and "date_from_format" {1}. '
            'Error: {2}'.format(date_from, date_from_format, err)
        )
    try:
        end_date = get_datetime(date_to, date_to_format)
    except Exception as err:
        raise ConfigurationError(
            'Unable to parse "date_to" {0} and "date_to_format" {1}. '
            'Error: {2}'.format(date_to, date_to_format, err)
        )
    # We have to iterate to one more month, and then subtract a second to get
    # the last day of the correct month
    if unit == 'months':
        month = end_date.month
        year = end_date.year
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
        new_end_date = datetime(year, month, 1, 0, 0, 0)
        end_epoch = datetime_to_epoch(new_end_date) - 1
    # Similarly, with years, we need to get the last moment of the year
    elif unit == 'years':
        new_end_date = datetime(end_date.year + 1, 1, 1, 0, 0, 0)
        end_epoch = datetime_to_epoch(new_end_date) - 1
    # It's not months or years, which have inconsistent reckoning...
    else:
        # This lets us use an existing method to simply add 1 more unit's worth
        # of seconds to get hours, days, or weeks, as they don't change
        # We use -1 as point of reference normally subtracts from the epoch
        # and we need to add to it, so we'll make it subtract a negative value.
        # Then, as before, subtract 1 to get the end of the period
        end_epoch = get_point_of_reference(
            unit, -1, epoch=datetime_to_epoch(end_date)) -1

    logger.debug('End ISO8601 = {0}'.format(
        datetime.utcfromtimestamp(end_epoch).isoformat()))
    return (start_epoch, end_epoch)

def parse_date_pattern(name):
    """
    Scan and parse `name` for :py:func:`time.strftime` strings, replacing them
    with the associated value when found, but otherwise returning lowercase
    values, as uppercase snapshot names are not allowed. It will detect if the
    first character is a `<`, which would indicate `name` is going to be using
    Elasticsearch date math syntax, and skip accordingly.

    The :py:func:`time.strftime` identifiers that Curator currently recognizes
    as acceptable include:

    * ``Y``: A 4 digit year
    * ``y``: A 2 digit year
    * ``m``: The 2 digit month
    * ``W``: The 2 digit week of the year
    * ``d``: The 2 digit day of the month
    * ``H``: The 2 digit hour of the day, in 24 hour notation
    * ``M``: The 2 digit minute of the hour
    * ``S``: The 2 digit number of second of the minute
    * ``j``: The 3 digit day of the year

    :arg name: A name, which can contain :py:func:`time.strftime`
        strings
    """
    logger = logging.getLogger(__name__)
    prev, curr, rendered = '', '', ''
    for curr in name:
        if curr == '<':
            logger.info('"{0}" is using Elasticsearch date math.'.format(name))
            rendered = name
            break
        if curr == '%':
            pass
        elif curr in date_regex() and prev == '%':
            rendered += str(datetime.utcnow().strftime('%{0}'.format(curr)))
        else:
            rendered += curr
        logger.debug('Partially rendered name: {0}'.format(rendered))
        prev = curr
    logger.debug('Fully rendered name: {0}'.format(rendered))
    return rendered

def get_datemath(client, datemath, random_element=None):
    """
    Return the parsed index name from ``datemath``
    """
    logger = logging.getLogger(__name__)
    if random_element is None:
        randomprefix = (
            'curator_get_datemath_function_' +
            ''.join(random.choice(string.ascii_lowercase) for _ in range(32))
        )
    else:
        randomprefix = 'curator_get_datemath_function_' + random_element
    datemath_dummy = '<{0}-{1}>'.format(randomprefix, datemath)
    # We both want and expect a 404 here (NotFoundError), since we have
    # created a 32 character random string to definitely be an unknown
    # index name.
    logger.debug('Random datemath string for extraction: {0}'.format(datemath_dummy))
    try:
        client.indices.get(index=datemath_dummy)
    except NotFoundError as err:
        # This is the magic.  Elasticsearch still gave us the formatted
        # index name in the error results.
        fauxindex = err.info['error']['index']
    except Exception: # any other exception
        raise ConfigurationError(
            'The datemath string "{0}" does not contain a valid date pattern '
            'or has invalid characters.'.format(datemath)
        )
    logger.debug('Response index name for extraction: {0}'.format(fauxindex))
    # Now we strip the random index prefix back out again
    pattern = r'^{0}-(.*)$'.format(randomprefix)
    rxp = re.compile(pattern)
    return rxp.match(fauxindex).group(1)

def isdatemath(data):
    """Check if a string is datemath"""
    logger = logging.getLogger(__name__)
    initial_check = r'^(.).*(.)$'
    rxp = re.compile(initial_check)
    opener = rxp.match(data).group(1)
    closer = rxp.match(data).group(2)
    logger.debug('opener =  {0}, closer = {1}'.format(opener, closer))
    if (opener == '<' and closer != '>') or (opener != '<' and closer == '>'):
        raise ConfigurationError('Incomplete datemath encapsulation in "< >"')
    elif (opener != '<' and closer != '>'):
        return False
    return True

def parse_datemath(client, value):
    """
    Check if ``value`` is datemath.
    Parse it if it is.
    Return the bare value otherwise.
    """
    logger = logging.getLogger(__name__)
    if not isdatemath(value):
        return value
    else:
        logger.debug('Properly encapsulated, proceeding to next evaluation...')
    # Our pattern has 4 capture groups.
    # 1. Everything after the initial '<' up to the first '{', which we call ``prefix``
    # 2. Everything between the outermost '{' and '}', which we call ``datemath``
    # 3. An optional inner '{' and '}' containing a date formatter and potentially a timezone.
    #    Not captured.
    # 4. Everything after the last '}' up to the closing '>'
    pattern = r'^<([^\{\}]*)?(\{.*(\{.*\})?\})([^\{\}]*)?>$'
    rxp = re.compile(pattern)
    try:
        prefix = rxp.match(value).group(1) or ''
        logger.debug('prefix = {0}'.format(prefix))
        datemath = rxp.match(value).group(2)
        logger.debug('datemath = {0}'.format(datemath))
        # formatter = rxp.match(value).group(3) or '' (not captured, but counted)
        suffix = rxp.match(value).group(4) or ''
        logger.debug('suffix = {0}'.format(suffix))
    except AttributeError:
        raise ConfigurationError(
            'Value "{0}" does not contain a valid datemath pattern.'.format(value))
    return '{0}{1}{2}'.format(prefix, get_datemath(client, datemath), suffix)
