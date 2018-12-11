# import logging
# from curator.actions.parentclasses import ActionClass
# from curator.helpers.index import chunk_index_list, verify_index_list
# from curator.helpers.utils import to_csv

# class Open(ActionClass):
#     def __init__(self, ilo):
#         """
#         :arg ilo: A :class:`curator.indexlist.IndexList` object
#         """
#         verify_index_list(ilo)
#         #: Instance variable.
#         #: The Elasticsearch Client object derived from `ilo`
#         self.client     = ilo.client
#         #: Instance variable.
#         #: Internal reference to `ilo`
#         self.index_list = ilo
#         self.loggit     = logging.getLogger('curator.actions.open')

#     def do_dry_run(self):
#         """
#         Log what the output would be, but take no action.
#         """
#         self.show_dry_run(self.index_list, 'open')

#     def do_action(self):
#         """
#         Open closed indices in `index_list.indices`
#         """
#         self.index_list.empty_list_check()
#         self.loggit.info(
#             'Opening selected indices: {0}'.format(self.index_list.indices))
#         try:
#             index_lists = chunk_index_list(self.index_list.indices)
#             for l in index_lists:
#                 self.client.indices.open(index=to_csv(l))
#         except Exception as e:
#             self.report_failure(e)

