import logging
from curator.actions.parentclasses import ActionClass
from curator.exceptions import ActionError, MissingArgument, CuratorException, SnapshotInProgress, FailedSnapshot
from curator.helpers.datemath import parse_date_pattern, parse_datemath
from curator.helpers.index import verify_index_list
from curator.helpers.repository import repository_exists, check_repo_fs
from curator.helpers.snapshot import create_snapshot_body, snapshot_running
from curator.helpers.waiting import wait_for_it

class Snapshot(ActionClass):
    def __init__(self, ilo, repository=None, name=None,
                ignore_unavailable=False, include_global_state=True,
                partial=False, wait_for_completion=True, wait_interval=9,
                max_wait=-1, skip_repo_fs_check=False):
        """
        :arg ilo: A :class:`curator.indexlist.IndexList` object
        :arg repository: The Elasticsearch snapshot repository to use
        :arg name: What to name the snapshot.
        :arg wait_for_completion: Wait (or not) for the operation
            to complete before returning.  (default: `True`)
        :type wait_for_completion: bool
        :arg wait_interval: How long in seconds to wait between checks for
            completion.
        :arg max_wait: Maximum number of seconds to `wait_for_completion`
        :arg ignore_unavailable: Ignore unavailable shards/indices.
            (default: `False`)
        :type ignore_unavailable: bool
        :arg include_global_state: Store cluster global state with snapshot.
            (default: `True`)
        :type include_global_state: bool
        :arg partial: Do not fail if primary shard is unavailable. (default:
            `False`)
        :type partial: bool
        :arg skip_repo_fs_check: Do not validate write access to repository on
            all cluster nodes before proceeding. (default: `False`).  Useful for
            shared filesystems where intermittent timeouts can affect
            validation, but won't likely affect snapshot success.
        :type skip_repo_fs_check: bool
        """
        verify_index_list(ilo)
        # Check here and don't bother with the rest of this if there are no
        # indices in the index list.
        ilo.empty_list_check()
        if not repository_exists(ilo.client, repository=repository):
            raise ActionError(
                'Cannot snapshot indices to missing repository: '
                '{0}'.format(repository)
            )
        if not name:
            raise MissingArgument('No value for "name" provided.')
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client              = ilo.client
        #: Instance variable.
        #: The parsed version of `name`
        self.name = parse_datemath(self.client, parse_date_pattern(name))
        #: Instance variable.
        #: Internal reference to `ilo`
        self.index_list = ilo
        #: Instance variable.
        #: Internally accessible copy of `repository`
        self.repository          = repository
        #: Instance variable.
        #: Internally accessible copy of `wait_for_completion`
        self.wait_for_completion = wait_for_completion
        #: Instance variable
        #: How many seconds to wait between checks for completion.
        self.wait_interval = wait_interval
        #: Instance variable.
        #: How long in seconds to `wait_for_completion` before returning with an
        #: exception. A value of -1 means wait forever.
        self.max_wait   = max_wait
        #: Instance variable.
        #: Internally accessible copy of `skip_repo_fs_check`
        self.skip_repo_fs_check  = skip_repo_fs_check
        self.state               = None

        #: Instance variable.
        #: Populated at instance creation time by calling
        #: :mod:`curator.utils.create_snapshot_body` with `ilo.indices` and the
        #: provided arguments: `ignore_unavailable`, `include_global_state`,
        #: `partial`
        self.body                = create_snapshot_body(
                ilo.indices,
                ignore_unavailable=ignore_unavailable,
                include_global_state=include_global_state,
                partial=partial
            )

        self.loggit = logging.getLogger('curator.actions.snapshot')

    def get_state(self):
        """
        Get the state of the snapshot
        """
        try:
            self.state = self.client.snapshot.get(
                repository=self.repository,
                snapshot=self.name)['snapshots'][0]['state']
            return self.state
        except IndexError:
            raise CuratorException(
                'Snapshot "{0}" not found in repository '
                '"{1}"'.format(self.name, self.repository)
            )

    def report_state(self):
        """
        Log the state of the snapshot and raise an exception if the state is
        not ``SUCCESS``
        """
        self.get_state()
        if self.state == 'SUCCESS':
            self.loggit.info('Snapshot {0} successfully completed.'.format(self.name))
        else:
            msg = 'Snapshot {0} completed with state: {0}'.format(self.state)
            self.loggit.error(msg)
            raise FailedSnapshot(msg)

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        self.loggit.info(
            'DRY-RUN: snapshot: {0} in repository {1} with arguments: '
            '{2}'.format(self.name, self.repository, self.body)
        )

    def do_action(self):
        """
        Snapshot indices in `index_list.indices`, with options passed.
        """
        if not self.skip_repo_fs_check:
            check_repo_fs(self.client, self.repository)
        if snapshot_running(self.client):
            raise SnapshotInProgress('Snapshot already in progress.')
        try:
            self.loggit.info('Creating snapshot "{0}" from indices: '
                '{1}'.format(self.name, self.index_list.indices)
            )
            # Always set wait_for_completion to False. Let 'wait_for_it' do its
            # thing if wait_for_completion is set to True. Report the task_id
            # either way.
            self.client.snapshot.create(
                repository=self.repository, snapshot=self.name, body=self.body,
                wait_for_completion=False
            )
            if self.wait_for_completion:
                wait_for_it(
                    self.client, 'snapshot', snapshot=self.name,
                    repository=self.repository,
                    wait_interval=self.wait_interval, max_wait=self.max_wait
                )
                self.report_state()
            else:
                self.loggit.warn(
                    '"wait_for_completion" set to {0}.'
                    'Remember to check for successful completion '
                    'manually.'.format(self.wait_for_completion)
                )
        except Exception as e:
            self.report_failure(e)
