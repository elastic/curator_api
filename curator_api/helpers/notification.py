import logging
from curator_api.exceptions import FailedExecution

logger = logging.getLogger(__name__)

def report_failure(exception):
    """
    Raise a `FailedExecution` exception and include the original error message.

    :arg exception: The upstream exception.
    :rtype: None
    """
    raise FailedExecution(
        'Exception encountered.  Rerun with loglevel DEBUG and/or check '
        'Elasticsearch logs for more information. '
        'Exception: {0}'.format(exception)
    )