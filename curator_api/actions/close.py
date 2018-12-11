# import logging
# from curator.actions.parentclasses import ActionClass
# from curator.helpers.index import chunk_index_list, verify_index_list
# from curator.helpers.utils import to_csv

# class Close(ActionClass):
#     def __init__(self, ilo, delete_aliases=False):
#         """
#         :arg ilo: A :class:`curator.indexlist.IndexList` object
#         :arg delete_aliases: If `True`, will delete any associated aliases
#             before closing indices.
#         :type delete_aliases: bool
#         """
#         verify_index_list(ilo)
#         #: Instance variable.
#         #: Internal reference to `ilo`
#         self.index_list = ilo
#         #: Instance variable.
#         #: Internal reference to `delete_aliases`
#         self.delete_aliases = delete_aliases
#         #: Instance variable.
#         #: The Elasticsearch Client object derived from `ilo`
#         self.client     = ilo.client
#         self.loggit     = logging.getLogger('curator.actions.close')

#     def do_dry_run(self):
#         """
#         Log what the output would be, but take no action.
#         """
#         self.show_dry_run(
#             self.index_list, 'close', **{'delete_aliases':self.delete_aliases})

#     def do_action(self):
#         """
#         Close open indices in `index_list.indices`
#         """
#         self.index_list.filter_closed()
#         self.index_list.empty_list_check()
#         self.loggit.info(
#             'Closing selected indices: {0}'.format(self.index_list.indices))
#         try:
#             index_lists = chunk_index_list(self.index_list.indices)
#             for l in index_lists:
#                 if self.delete_aliases:
#                     self.loggit.info(
#                         'Deleting aliases from indices before closing.')
#                     self.loggit.debug('Deleting aliases from: {0}'.format(l))
#                     try:
#                         self.client.indices.delete_alias(
#                             index=to_csv(l), name='_all')
#                     except Exception as e:
#                         self.loggit.warn(
#                             'Some indices may not have had aliases.  Exception:'
#                             ' {0}'.format(e)
#                         )
#                 self.client.indices.flush_synced(
#                     index=to_csv(l), ignore_unavailable=True)
#                 self.client.indices.close(
#                     index=to_csv(l), ignore_unavailable=True)
#         except Exception as e:
#             self.report_failure(e)
