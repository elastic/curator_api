"""
This includes the parent class for all other actions.
"""

import logging
from curator_api.exceptions import FailedExecution

class ActionClass(object):
    """ActionClass Super Class"""
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def show_dry_run(self, ilo, action, **kwargs):
        """
        Log dry run output with the action which would have been executed.

        :arg ilo: A :class:`curator.indexlist.IndexList`
        :arg action: The `action` to be performed.
        :arg kwargs: Any other args to show in the log output
        """
        self.logger.info('DRY-RUN MODE.  No changes will be made.')
        self.logger.info(
            '(CLOSED) indices may be shown that may not be acted on by '
            'action "{0}".'.format(action)
        )
        indices = sorted(ilo.indices)
        for idx in indices:
                index_closed = ilo.index_info[idx]['state'] == 'close'
                self.logger.info(
                    'DRY-RUN: {0}: {1}{2} with arguments: {3}'.format(
                        action, idx, ' (CLOSED)' if index_closed else '', kwargs
                    )
                )

    def report_failure(self, exception):
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
