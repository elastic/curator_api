# import logging
# from curator.actions.parentclasses import ActionClass
# from curator.helpers.client import verify_client_object
# from curator.helpers.waiting import wait_for_it

# class ClusterRouting(ActionClass):
#     def __init__(
#         self, client, routing_type=None, setting=None, value=None,
#         wait_for_completion=False, wait_interval=9, max_wait=-1
#     ):
#         """
#         For now, the cluster routing settings are hardcoded to be ``transient``

#         :arg client: An :class:`elasticsearch.Elasticsearch` client object
#         :arg routing_type: Type of routing to apply. Either `allocation` or
#             `rebalance`
#         :arg setting: Currently, the only acceptable value for `setting` is
#             ``enable``. This is here in case that changes.
#         :arg value: Used only if `setting` is `enable`. Semi-dependent on
#             `routing_type`. Acceptable values for `allocation` and `rebalance`
#             are ``all``, ``primaries``, and ``none`` (string, not `NoneType`).
#             If `routing_type` is `allocation`, this can also be
#             ``new_primaries``, and if `rebalance`, it can be ``replicas``.
#         :arg wait_for_completion: Wait (or not) for the operation
#             to complete before returning.  (default: `False`)
#         :type wait_for_completion: bool
#         :arg wait_interval: How long in seconds to wait between checks for
#             completion.
#         :arg max_wait: Maximum number of seconds to `wait_for_completion`
#         """
#         verify_client_object(client)
#         #: Instance variable.
#         #: An :class:`elasticsearch.Elasticsearch` client object
#         self.client  = client
#         self.loggit  = logging.getLogger('curator.actions.cluster_routing')
#         #: Instance variable.
#         #: Internal reference to `wait_for_completion`
#         self.wfc     = wait_for_completion
#         #: Instance variable
#         #: How many seconds to wait between checks for completion.
#         self.wait_interval = wait_interval
#         #: Instance variable.
#         #: How long in seconds to `wait_for_completion` before returning with an
#         #: exception. A value of -1 means wait forever.
#         self.max_wait   = max_wait

#         if setting != 'enable':
#             raise ValueError(
#                 'Invalid value for "setting": {0}.'.format(setting)
#             )
#         if routing_type == 'allocation':
#             if value not in ['all', 'primaries', 'new_primaries', 'none']:
#                 raise ValueError(
#                     'Invalid "value": {0} with "routing_type":'
#                     '{1}.'.format(value, routing_type)
#                 )
#         elif routing_type == 'rebalance':
#             if value not in ['all', 'primaries', 'replicas', 'none']:
#                 raise ValueError(
#                     'Invalid "value": {0} with "routing_type":'
#                     '{1}.'.format(value, routing_type)
#                 )
#         else:
#             raise ValueError(
#                 'Invalid value for "routing_type": {0}.'.format(routing_type)
#             )
#         bkey = 'cluster.routing.{0}.{1}'.format(routing_type,setting)
#         self.body = { 'transient' : { bkey : value } }

#     def do_dry_run(self):
#         """
#         Log what the output would be, but take no action.
#         """
#         self.loggit.info('DRY-RUN MODE.  No changes will be made.')
#         self.loggit.info(
#             'DRY-RUN: Update cluster routing settings with arguments: '
#             '{0}'.format(self.body)
#         )

#     def do_action(self):
#         """
#         Change cluster routing settings with the settings in `body`.
#         """
#         self.loggit.info('Updating cluster settings: {0}'.format(self.body))
#         try:
#             self.client.cluster.put_settings(body=self.body)
#             if self.wfc:
#                 self.loggit.debug(
#                     'Waiting for shards to complete routing and/or rebalancing'
#                 )
#                 wait_for_it(
#                     self.client, 'cluster_routing',
#                     wait_interval=self.wait_interval, max_wait=self.max_wait
#                 )
#         except Exception as e:
#             self.report_failure(e)
