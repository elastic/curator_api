import logging
import time
from curator.actions.parentclasses import ActionClass
from curator.exceptions import MissingArgument
from curator.helpers.index import verify_index_list

class ForceMerge(ActionClass):
    def __init__(self, ilo, max_num_segments=None, delay=0):
        """
        :arg ilo: A :class:`curator.indexlist.IndexList` object
        :arg max_num_segments: Number of segments per shard to forceMerge
        :arg delay: Number of seconds to delay between forceMerge operations
        """
        verify_index_list(ilo)
        if not max_num_segments:
            raise MissingArgument('Missing value for "max_num_segments"')
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client = ilo.client
        #: Instance variable.
        #: Internal reference to `ilo`
        self.index_list = ilo
        #: Instance variable.
        #: Internally accessible copy of `max_num_segments`
        self.max_num_segments = max_num_segments
        #: Instance variable.
        #: Internally accessible copy of `delay`
        self.delay = delay
        self.loggit = logging.getLogger('curator.actions.forcemerge')

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        self.show_dry_run(
            self.index_list, 'forcemerge',
            max_num_segments=self.max_num_segments,
            delay=self.delay,
        )

    def do_action(self):
        """
        forcemerge indices in `index_list.indices`
        """
        self.index_list.filter_closed()
        self.index_list.filter_forceMerged(
            max_num_segments=self.max_num_segments)
        self.index_list.empty_list_check()
        self.loggit.info('forceMerging selected indices')
        try:
            for index_name in self.index_list.indices:
                self.loggit.info(
                    'forceMerging index {0} to {1} segments per shard.  '
                    'Please wait...'.format(index_name, self.max_num_segments)
                )
                self.client.indices.forcemerge(index=index_name,
                    max_num_segments=self.max_num_segments)
                if self.delay > 0:
                    self.loggit.info(
                        'Pausing for {0} seconds before continuing...'.format(
                            self.delay)
                    )
                    time.sleep(self.delay)
        except Exception as e:
            self.report_failure(e)
