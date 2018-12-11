# import logging
# from curator.actions.parentclasses import ActionClass
# from curator.exceptions import ConfigurationError
# from curator.helpers.datemath import parse_date_pattern
# from curator.helpers.client import verify_client_object

# class CreateIndex(ActionClass):
#     def __init__(self, client, name, extra_settings={}):
#         """
#         :arg client: An :class:`elasticsearch.Elasticsearch` client object
#         :arg name: A name, which can contain :py:func:`time.strftime`
#             strings
#         :arg extra_settings: The `settings` and `mappings` for the index. For
#             more information see
#             https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html
#         :type extra_settings: dict, representing the settings and mappings.
#         """
#         verify_client_object(client)
#         if not name:
#             raise ConfigurationError('Value for "name" not provided.')
#         #: Instance variable.
#         #: The parsed version of `name`
#         self.name       = parse_date_pattern(name)
#         #: Instance variable.
#         #: Extracted from the config yaml, it should be a dictionary of
#         #: mappings and settings suitable for index creation.
#         self.body       = extra_settings
#         #: Instance variable.
#         #: An :class:`elasticsearch.Elasticsearch` client object
#         self.client     = client
#         self.loggit     = logging.getLogger('curator.actions.createindex')

#     def do_dry_run(self):
#         """
#         Log what the output would be, but take no action.
#         """
#         self.loggit.info('DRY-RUN MODE.  No changes will be made.')
#         self.loggit.info(
#             'DRY-RUN: create_index "{0}" with arguments: '
#             '{1}'.format(self.name, self.body)
#         )

#     def do_action(self):
#         """
#         Create index identified by `name` with settings in `body`
#         """
#         self.loggit.info(
#             'Creating index "{0}" with settings: '
#             '{1}'.format(self.name, self.body)
#         )
#         try:
#             self.client.indices.create(index=self.name, body=self.body)
#         except Exception as e:
#             self.report_failure(e)
