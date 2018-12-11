"""Utility functions"""
# from sys import version_info as python_version
import logging
from six import string_types
from curator_api.exceptions import NoIndices

def byte_size(num, suffix='B'):
    """
    Return a formatted string indicating the size in bytes, with the proper
    unit, e.g. KB, MB, GB, TB, etc.

    :arg num: The number of byte
    :arg suffix: An arbitrary suffix, like `Bytes`
    :rtype: float
    """
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Y', suffix)

def ensure_list(indices):
    """
    Return a list, even if indices is a single value

    :arg indices: A list of indices to act upon
    :rtype: list
    """
    if not isinstance(indices, list): # in case of a single value passed
        indices = [indices]
    return indices

def empty_list_check(index_list):
    """Raise exception if `index_list` is empty"""
    logger = logging.getLogger(__name__)
    logger.debug('Checking for empty index list')
    if not index_list:
        raise NoIndices('index_list object is empty.')

def to_csv(indices):
    """
    Return a csv string from a list of indices, or a single value if only one
    value is present

    :arg indices: A list of indices to act on, or a single value, which could be
        in the format of a csv string already.
    :rtype: str
    """
    indices = ensure_list(indices) # in case of a single value passed
    if indices:
        return ','.join(sorted(indices))
    else:
        return None

def prune_nones(mydict):
    """
    Remove keys from `mydict` whose values are `None`

    :arg mydict: The dictionary to act on
    :rtype: dict
    """
    # Test for `None` instead of existence or zero values will be caught
    return dict([(k, v) for k, v in mydict.items() if v != None and v != 'None'])

def check_csv(value):
    """
    Some of the curator methods should not operate against multiple indices at
    once.  This method can be used to check if a list or csv has been sent.

    :arg value: The value to test, if list or csv string
    :rtype: bool
    """
    if isinstance(value, list):
        return True
    # # Python3 hack because it doesn't recognize unicode as a type anymore
    # if python_version < (3, 0):
    #     if isinstance(value, unicode):
    #         value = str(value)
    if isinstance(value, string_types):
        if len(value.split(',')) > 1: # It's a csv string.
            return True
        else: # There's only one value here, so it's not a csv string
            return False
    else:
        raise TypeError(
            'Passed value: {0} is not a list or a string but is of type {1}'.format(
                value, type(value))
        )
