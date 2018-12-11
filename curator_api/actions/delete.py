# import logging
# from curator.actions.parentclasses import ActionClass
# from curator.exceptions import FailedExecution
# from curator.helpers.index import chunk_index_list, get_indices, verify_index_list
# from curator.helpers.snapshot import safe_to_snap, verify_snapshot_list
# from curator.helpers.utils import to_csv
# from curator.helpers.waiting import wait_for_it

# class IndexDelete(ActionClass):
#     def __init__(self, ilo, master_timeout=30):
#         """
#         :arg ilo: A :class:`curator.indexlist.IndexList` object
#         :arg master_timeout: Number of seconds to wait for master node response
#         """
#         verify_index_list(ilo)
#         if not isinstance(master_timeout, int):
#             raise TypeError(
#                 'Incorrect type for "master_timeout": {0}. '
#                 'Should be integer value.'.format(type(master_timeout))
#             )
#         #: Instance variable.
#         #: Internal reference to `ilo`
#         self.index_list     = ilo
#         #: Instance variable.
#         #: The Elasticsearch Client object derived from `ilo`
#         self.client         = ilo.client
#         #: Instance variable.
#         #: String value of `master_timeout` + 's', for seconds.
#         self.master_timeout = str(master_timeout) + 's'
#         self.loggit         = logging.getLogger('curator.actions.delete_indices')
#         self.loggit.debug('master_timeout value: {0}'.format(
#             self.master_timeout))

#     def _verify_result(self, result, count):
#         """
#         Breakout method to aid readability
#         :arg result: A list of indices from `_get_result_list`
#         :arg count: The number of tries that have occurred
#         :rtype: bool
#         """
#         if len(result) > 0:
#             self.loggit.error(
#                 'The following indices failed to delete on try '
#                 '#{0}:'.format(count)
#             )
#             for idx in result:
#                 self.loggit.error("---{0}".format(idx))
#             return False
#         else:
#             self.loggit.debug(
#                 'Successfully deleted all indices on try #{0}'.format(count)
#             )
#             return True

#     def __chunk_loop(self, chunk_list):
#         """
#         Loop through deletes 3 times to ensure they complete
#         :arg chunk_list: A list of indices pre-chunked so it won't overload the
#             URL size limit.
#         """
#         working_list = chunk_list
#         for count in range(1, 4): # Try 3 times
#             for i in working_list:
#                 self.loggit.info("---deleting index {0}".format(i))
#             self.client.indices.delete(
#                 index=to_csv(working_list), master_timeout=self.master_timeout)
#             result = [ i for i in working_list if i in get_indices(self.client)]
#             if self._verify_result(result, count):
#                 return
#             else:
#                 working_list = result
#         self.loggit.error(
#             'Unable to delete the following indices after 3 attempts: '
#             '{0}'.format(result)
#         )

#     def do_dry_run(self):
#         """
#         Log what the output would be, but take no action.
#         """
#         self.show_dry_run(self.index_list, 'delete_indices')

#     def do_action(self):
#         """
#         Delete indices in `index_list.indices`
#         """
#         self.index_list.empty_list_check()
#         self.loggit.info(
#             'Deleting selected indices: {0}'.format(self.index_list.indices))
#         try:
#             index_lists = chunk_index_list(self.index_list.indices)
#             for l in index_lists:
#                 self.__chunk_loop(l)
#         except Exception as e:
#             self.report_failure(e)

# class SnapshotDelete(ActionClass):
#     def __init__(self, slo, retry_interval=120, retry_count=3):
#         """
#         :arg slo: A :class:`curator.snapshotlist.SnapshotList` object
#         :arg retry_interval: Number of seconds to delay betwen retries. Default:
#             120 (seconds)
#         :arg retry_count: Number of attempts to make. Default: 3
#         """
#         verify_snapshot_list(slo)
#         #: Instance variable.
#         #: The Elasticsearch Client object derived from `slo`
#         self.client         = slo.client
#         #: Instance variable.
#         #: Internally accessible copy of `retry_interval`
#         self.retry_interval = retry_interval
#         #: Instance variable.
#         #: Internally accessible copy of `retry_count`
#         self.retry_count    = retry_count
#         #: Instance variable.
#         #: Internal reference to `slo`
#         self.snapshot_list  = slo
#         #: Instance variable.
#         #: The repository name derived from `slo`
#         self.repository     = slo.repository
#         self.loggit = logging.getLogger('curator.actions.delete_snapshots')

#     def do_dry_run(self):
#         """
#         Log what the output would be, but take no action.
#         """
#         self.loggit.info('DRY-RUN MODE.  No changes will be made.')
#         mykwargs = {
#             'repository' : self.repository,
#             'retry_interval' : self.retry_interval,
#             'retry_count' : self.retry_count,
#         }
#         for snap in self.snapshot_list.snapshots:
#             self.loggit.info('DRY-RUN: delete_snapshot: {0} with arguments: '
#                 '{1}'.format(snap, mykwargs))

#     def do_action(self):
#         """
#         Delete snapshots in `slo`
#         Retry up to `retry_count` times, pausing `retry_interval`
#         seconds between retries.
#         """
#         self.snapshot_list.empty_list_check()
#         self.loggit.info('Deleting selected snapshots')
#         if not safe_to_snap(
#             self.client, repository=self.repository,
#             retry_interval=self.retry_interval, retry_count=self.retry_count):
#                 raise FailedExecution(
#                     'Unable to delete snapshot(s) because a snapshot is in '
#                     'state "IN_PROGRESS"')
#         try:
#             for s in self.snapshot_list.snapshots:
#                 self.loggit.info('Deleting snapshot {0}...'.format(s))
#                 self.client.snapshot.delete(
#                     repository=self.repository, snapshot=s)
#         except Exception as e:
#             self.report_failure(e)
