import logging
import re
from curator.actions.parentclasses import ActionClass
from curator.exceptions import CuratorException, SnapshotInProgress, FailedRestore
from curator.helpers.index import get_indices
from curator.helpers.repository import check_repo_fs
from curator.helpers.snapshot import snapshot_running, verify_snapshot_list
from curator.helpers.utils import ensure_list
from curator.helpers.waiting import wait_for_it

class Restore(ActionClass):
    def __init__(self, slo, name=None, indices=None, include_aliases=False,
                ignore_unavailable=False, include_global_state=False,
                partial=False, rename_pattern=None, rename_replacement=None,
                extra_settings={}, wait_for_completion=True, wait_interval=9,
                max_wait=-1, skip_repo_fs_check=False):
        """
        :arg slo: A :class:`curator.snapshotlist.SnapshotList` object
        :arg name: Name of the snapshot to restore.  If no name is provided, it
            will restore the most recent snapshot by age.
        :type name: str
        :arg indices: A list of indices to restore.  If no indices are provided,
            it will restore all indices in the snapshot.
        :type indices: list
        :arg include_aliases: If set to `True`, restore aliases with the
            indices. (default: `False`)
        :type include_aliases: bool
        :arg ignore_unavailable: Ignore unavailable shards/indices.
            (default: `False`)
        :type ignore_unavailable: bool
        :arg include_global_state: Restore cluster global state with snapshot.
            (default: `False`)
        :type include_global_state: bool
        :arg partial: Do not fail if primary shard is unavailable. (default:
            `False`)
        :type partial: bool
        :arg rename_pattern: A regular expression pattern with one or more
            captures, e.g. ``index_(.+)``
        :type rename_pattern: str
        :arg rename_replacement: A target index name pattern with `$#` numbered
            references to the captures in ``rename_pattern``, e.g.
            ``restored_index_$1``
        :type rename_replacement: str
        :arg extra_settings: Extra settings, including shard count and settings
            to omit. For more information see
            https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html#_changing_index_settings_during_restore
        :type extra_settings: dict, representing the settings.
        :arg wait_for_completion: Wait (or not) for the operation
            to complete before returning.  (default: `True`)
        :arg wait_interval: How long in seconds to wait between checks for
            completion.
        :arg max_wait: Maximum number of seconds to `wait_for_completion`
        :type wait_for_completion: bool

        :arg skip_repo_fs_check: Do not validate write access to repository on
            all cluster nodes before proceeding. (default: `False`).  Useful for
            shared filesystems where intermittent timeouts can affect
            validation, but won't likely affect snapshot success.
        :type skip_repo_fs_check: bool
        """
        self.loggit = logging.getLogger('curator.actions.snapshot')
        verify_snapshot_list(slo)
        # Get the most recent snapshot.
        most_recent = slo.most_recent()
        self.loggit.debug('"most_recent" snapshot: {0}'.format(most_recent))
        #: Instance variable.
        #: Will use a provided snapshot name, or the most recent snapshot in slo
        self.name = name if name else most_recent
        # Stop here now, if it's not a successful snapshot.
        if slo.snapshot_info[self.name]['state'] == 'PARTIAL' \
            and partial == True:
            self.loggit.warn(
                'Performing restore of snapshot in state PARTIAL.')
        elif slo.snapshot_info[self.name]['state'] != 'SUCCESS':
            raise CuratorException(
                'Restore operation can only be performed on snapshots with '
                'state "SUCCESS", or "PARTIAL" if partial=True.'
            )
        #: Instance variable.
        #: The Elasticsearch Client object derived from `slo`
        self.client              = slo.client
        #: Instance variable.
        #: Internal reference to `slo`
        self.snapshot_list = slo
        #: Instance variable.
        #: `repository` derived from `slo`
        self.repository          = slo.repository

        if indices:
            self.indices = ensure_list(indices)
        else:
            self.indices = slo.snapshot_info[self.name]['indices']
        self.wfc                 = wait_for_completion
        #: Instance variable
        #: How many seconds to wait between checks for completion.
        self.wait_interval = wait_interval
        #: Instance variable.
        #: How long in seconds to `wait_for_completion` before returning with an
        #: exception. A value of -1 means wait forever.
        self.max_wait   = max_wait
        #: Instance variable version of ``rename_pattern``
        self.rename_pattern = rename_pattern if rename_replacement is not None \
            else ''
        #: Instance variable version of ``rename_replacement``
        self.rename_replacement = rename_replacement if rename_replacement \
            is not None else ''
        #: Also an instance variable version of ``rename_replacement``
        #: but with Java regex group designations of ``$#``
        #: converted to Python's ``\\#`` style.
        self.py_rename_replacement = self.rename_replacement.replace('$', '\\')
        #: Instance variable.
        #: Internally accessible copy of `skip_repo_fs_check`
        self.skip_repo_fs_check  = skip_repo_fs_check

        #: Instance variable.
        #: Populated at instance creation time from the other options
        self.body                = {
                'indices' : self.indices,
                'include_aliases' : include_aliases,
                'ignore_unavailable' : ignore_unavailable,
                'include_global_state' : include_global_state,
                'partial' : partial,
                'rename_pattern' : self.rename_pattern,
                'rename_replacement' : self.rename_replacement,
            }
        if extra_settings:
            self.loggit.debug('Adding extra_settings to restore body: {0}'.format(extra_settings))
            try:
                self.body.update(extra_settings)
            except:
                self.loggit.error('Unable to apply extra settings to restore body')
        self.loggit.debug('REPOSITORY: {0}'.format(self.repository))
        self.loggit.debug('WAIT_FOR_COMPLETION: {0}'.format(self.wfc))
        self.loggit.debug('SKIP_REPO_FS_CHECK: {0}'.format(self.skip_repo_fs_check))
        self.loggit.debug('BODY: {0}'.format(self.body))
        # Populate the expected output index list.
        self._get_expected_output()

    def _get_expected_output(self):
        if not self.rename_pattern and not self.rename_replacement:
            self.expected_output = self.indices
            return # Don't stick around if we're not replacing anything
        self.expected_output = []
        for index in self.indices:
            self.expected_output.append(
                re.sub(
                    self.rename_pattern,
                    self.py_rename_replacement,
                    index
                )
            )
            self.loggit.debug('index: {0} replacement: {1}'.format(index, self.expected_output[-1]))

    def report_state(self):
        """
        Log the state of the restore
        This should only be done if ``wait_for_completion`` is `True`, and only
        after completing the restore.
        """
        all_indices = get_indices(self.client)
        found_count = 0
        missing = []
        for index in self.expected_output:
            if index in all_indices:
                found_count += 1
                self.loggit.info('Found restored index {0}'.format(index))
            else:
                missing.append(index)
        if found_count == len(self.expected_output):
            self.loggit.info('All indices appear to have been restored.')
        else:
            msg = 'Some of the indices do not appear to have been restored. Missing: {0}'.format(missing)
            self.loggit.error(msg)
            raise FailedRestore(msg)

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        self.loggit.info(
            'DRY-RUN: restore: Repository: {0} Snapshot name: {1} Arguments: '
            '{2}'.format(
                self.repository, self.name,
                { 'wait_for_completion' : self.wfc, 'body' : self.body }
            )
        )

        for index in self.indices:
            if self.rename_pattern and self.rename_replacement:
                replacement_msg = 'as {0}'.format(
                    re.sub(
                        self.rename_pattern,
                        self.py_rename_replacement,
                        index
                    )
                )
            else:
                replacement_msg = ''
            self.loggit.info('DRY-RUN: restore: Index {0} {1}'.format(index, replacement_msg))


    def do_action(self):
        """
        Restore indices with options passed.
        """
        if not self.skip_repo_fs_check:
            check_repo_fs(self.client, self.repository)
        if snapshot_running(self.client):
            raise SnapshotInProgress('Cannot restore while a snapshot is in progress.')
        try:
            self.loggit.info('Restoring indices "{0}" from snapshot: '
                '{1}'.format(self.indices, self.name)
            )
            # Always set wait_for_completion to False. Let 'wait_for_it' do its
            # thing if wait_for_completion is set to True. Report the task_id
            # either way.
            self.client.snapshot.restore(
                repository=self.repository, snapshot=self.name, body=self.body,
                wait_for_completion=False
            )
            if self.wfc:
                wait_for_it(
                    self.client, 'restore', index_list=self.expected_output,
                    wait_interval=self.wait_interval, max_wait=self.max_wait
                )
                self.report_state()
            else:
                self.loggit.warn(
                    '"wait_for_completion" set to {0}. '
                    'Remember to check for successful completion '
                    'manually.'.format(self.wfc)
                )
        except Exception as e:
            self.report_failure(e)
