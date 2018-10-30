import logging
from curator.actions.parentclasses import ActionClass
from curator.exceptions import MissingArgument
from curator.helpers.index import chunk_index_list, verify_index_list
from curator.helpers.utils import to_csv
from curator.helpers.waiting import wait_for_it

class Replicas(ActionClass):
    def __init__(self, ilo, count=None, wait_for_completion=False,
        wait_interval=9, max_wait=-1):
        """
        :arg ilo: A :class:`curator.indexlist.IndexList` object
        :arg count: The count of replicas per shard
        :arg wait_for_completion: Wait (or not) for the operation
            to complete before returning.  (default: `False`)
        :type wait_for_completion: bool
        :arg wait_interval: How long in seconds to wait between checks for
            completion.
        :arg max_wait: Maximum number of seconds to `wait_for_completion`
        """
        verify_index_list(ilo)
        # It's okay for count to be zero
        if count == 0:
            pass
        elif not count:
            raise MissingArgument('Missing value for "count"')
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client     = ilo.client
        #: Instance variable.
        #: Internal reference to `ilo`
        self.index_list = ilo
        #: Instance variable.
        #: Internally accessible copy of `count`
        self.count      = count
        #: Instance variable.
        #: Internal reference to `wait_for_completion`
        self.wfc        = wait_for_completion
        #: Instance variable
        #: How many seconds to wait between checks for completion.
        self.wait_interval = wait_interval
        #: Instance variable.
        #: How long in seconds to `wait_for_completion` before returning with an
        #: exception. A value of -1 means wait forever.
        self.max_wait   = max_wait
        self.loggit     = logging.getLogger('curator.actions.replicas')

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        self.show_dry_run(self.index_list, 'replicas', count=self.count)

    def do_action(self):
        """
        Update the replica count of indices in `index_list.indices`
        """
        self.loggit.debug(
            'Cannot get update replica count of closed indices.  '
            'Omitting any closed indices.'
        )
        self.index_list.filter_closed()
        self.index_list.empty_list_check()
        self.loggit.info(
            'Setting the replica count to {0} for indices: '
            '{1}'.format(self.count, self.index_list.indices)
        )
        try:
            index_lists = chunk_index_list(self.index_list.indices)
            for l in index_lists:
                self.client.indices.put_settings(index=to_csv(l),
                    body={'number_of_replicas' : self.count})
                if self.wfc and self.count > 0:
                    self.loggit.debug(
                        'Waiting for shards to complete replication for '
                        'indices: {0}'.format(to_csv(l))
                    )
                    wait_for_it(
                        self.client, 'replicas',
                        wait_interval=self.wait_interval, max_wait=self.max_wait
                    )
        except Exception as e:
            self.report_failure(e)
