# import logging
# from curator.actions.parentclasses import ActionClass
# from curator.exceptions import ActionError, ConfigurationError, CuratorException
# from curator.helpers.index import chunk_index_list, get_indices, index_size, verify_index_list
# from curator.helpers.node import name_to_node_id, node_id_to_name, node_roles, single_data_path
# from curator.helpers.waiting import health_check, wait_for_it
# from curator.helpers.utils import ensure_list

# class Shrink(ActionClass):
#     def __init__(self, ilo, shrink_node='DETERMINISTIC', node_filters={},
#                 number_of_shards=1, number_of_replicas=1,
#                 shrink_prefix='', shrink_suffix='-shrink',
#                 copy_aliases=False,
#                 delete_after=True, post_allocation={},
#                 wait_for_active_shards=1, wait_for_rebalance=True,
#                 extra_settings={}, wait_for_completion=True, wait_interval=9,
#                 max_wait=-1):
#         """
#         :arg ilo: A :class:`curator.indexlist.IndexList` object
#         :arg shrink_node: The node name to use as the shrink target, or
#             ``DETERMINISTIC``, which will use the values in ``node_filters`` to
#             determine which node will be the shrink node.
#         :arg node_filters: If the value of ``shrink_node`` is ``DETERMINISTIC``,
#             the values in ``node_filters`` will be used while determining which
#             node to allocate the shards on before performing the shrink.
#         :type node_filters: dict, representing the filters
#         :arg number_of_shards: The number of shards the shrunk index should have
#         :arg number_of_replicas: The number of replicas for the shrunk index
#         :arg shrink_prefix: Prepend the shrunk index with this value
#         :arg shrink_suffix: Append the value to the shrunk index (default: `-shrink`)
#         :arg copy_aliases: Whether to copy each source index aliases to target index after shrinking.
#             the aliases will be added to target index and deleted from source index at the same time(default: `False`)
#         :type copy_aliases: bool
#         :arg delete_after: Whether to delete each index after shrinking. (default: `True`)
#         :type delete_after: bool
#         :arg post_allocation: If populated, the `allocation_type`, `key`, and
#             `value` will be applied to the shrunk index to re-route it.
#         :type post_allocation: dict, with keys `allocation_type`, `key`, and `value`
#         :arg wait_for_active_shards: The number of shards expected to be active before returning.
#         :arg extra_settings:  Permitted root keys are `settings` and `aliases`.
#             See https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-shrink-index.html
#         :type extra_settings: dict
#         :arg wait_for_rebalance: Wait for rebalance. (default: `True`)
#         :type wait_for_rebalance: bool
#         :arg wait_for_active_shards: Wait for active shards before returning.
#         :arg wait_for_completion: Wait (or not) for the operation
#             to complete before returning.  You should not normally change this,
#             ever. (default: `True`)
#         :arg wait_interval: How long in seconds to wait between checks for
#             completion.
#         :arg max_wait: Maximum number of seconds to `wait_for_completion`
#         :type wait_for_completion: bool
#         """
#         self.loggit = logging.getLogger('curator.actions.shrink')
#         verify_index_list(ilo)
#         if not 'permit_masters' in node_filters:
#             node_filters['permit_masters'] = False
#         #: Instance variable. The Elasticsearch Client object derived from `ilo`
#         self.client           = ilo.client
#         #: Instance variable. Internal reference to `ilo`
#         self.index_list       = ilo
#         #: Instance variable. Internal reference to `shrink_node`
#         self.shrink_node      = shrink_node
#         #: Instance variable. Internal reference to `node_filters`
#         self.node_filters     = node_filters
#         #: Instance variable. Internal reference to `shrink_prefix`
#         self.shrink_prefix    = shrink_prefix
#         #: Instance variable. Internal reference to `shrink_suffix`
#         self.shrink_suffix    = shrink_suffix
#         #: Instance variable. Internal reference to `copy_aliases`
#         self.copy_aliases = copy_aliases
#         #: Instance variable. Internal reference to `delete_after`
#         self.delete_after     = delete_after
#         #: Instance variable. Internal reference to `post_allocation`
#         self.post_allocation  = post_allocation
#         #: Instance variable. Internal reference to `wait_for_rebalance`
#         self.wait_for_rebalance = wait_for_rebalance
#         #: Instance variable. Internal reference to `wait_for_completion`
#         self.wfc              = wait_for_completion
#         #: Instance variable. How many seconds to wait between checks for completion.
#         self.wait_interval    = wait_interval
#         #: Instance variable. How long in seconds to `wait_for_completion` before returning with an exception. A value of -1 means wait forever.
#         self.max_wait         = max_wait
#         #: Instance variable. Internal reference to `number_of_shards`
#         self.number_of_shards = number_of_shards
#         self.wait_for_active_shards = wait_for_active_shards
#         self.shrink_node_name = None
#         self.body = {
#             'settings': {
#                 'index.number_of_shards' : number_of_shards,
#                 'index.number_of_replicas' : number_of_replicas,
#             }
#         }
#         if extra_settings:
#             self._merge_extra_settings(extra_settings)

#     def _merge_extra_settings(self, extra_settings):
#         self.loggit.debug(
#             'Adding extra_settings to shrink body: '
#             '{0}'.format(extra_settings)
#         )
#         # Pop these here, otherwise we could overwrite our default number of
#         # shards and replicas
#         if 'settings' in extra_settings:
#             settings = extra_settings.pop('settings')
#             try:
#                 self.body['settings'].update(settings)
#             except Exception:
#                 raise ConfigurationError('Unable to apply extra settings "{0}" to shrink body'.format({'settings':settings}))
#         if extra_settings:
#             try: # Apply any remaining keys, should there be any.
#                 self.body.update(extra_settings)
#             except Exception:
#                 raise ConfigurationError('Unable to apply extra settings "{0}" to shrink body'.format(extra_settings))

#     def _data_node(self, node_id):
#         roles = node_roles(self.client, node_id)
#         name = node_id_to_name(self.client, node_id)
#         if not 'data' in roles:
#             self.loggit.info('Skipping node "{0}": non-data node'.format(name))
#             return False
#         if 'master' in roles and not self.node_filters['permit_masters']:
#             self.loggit.info('Skipping node "{0}": master node'.format(name))
#             return False
#         elif 'master' in roles and self.node_filters['permit_masters']:
#             self.loggit.warn('Not skipping node "{0}" which is a master node (not recommended), but permit_masters is True'.format(name))
#             return True
#         else: # It does have `data` as a role.
#             return True

#     def _exclude_node(self, name):
#         if 'exclude_nodes' in self.node_filters:
#             if name in self.node_filters['exclude_nodes']:
#                 self.loggit.info('Excluding node "{0}" due to node_filters'.format(name))
#                 return True
#         return False

#     def _shrink_target(self, name):
#         return '{0}{1}{2}'.format(self.shrink_prefix, name, self.shrink_suffix)

#     def qualify_single_node(self):
#         node_id = name_to_node_id(self.client, self.shrink_node)
#         if node_id:
#             self.shrink_node_id   = node_id
#             self.shrink_node_name = self.shrink_node
#         else:
#             raise ConfigurationError('Unable to find node named: "{0}"'.format(self.shrink_node))
#         if self._exclude_node(self.shrink_node):
#             raise ConfigurationError('Node "{0}" listed for exclusion'.format(self.shrink_node))
#         if not self._data_node(node_id):
#             raise ActionError('Node "{0}" is not usable as a shrink node'.format(self.shrink_node))
#         if not single_data_path(self.client, node_id):
#             raise ActionError(
#                 'Node "{0}" has multiple data paths and cannot be used '
#                 'for shrink operations.'
#                 .format(self.shrink_node)
#             )
#         self.shrink_node_avail = (
#             self.client.nodes.stats()['nodes'][node_id]['fs']['total']['available_in_bytes']
#         )

#     def most_available_node(self):
#         """
#         Determine which data node name has the most available free space, and
#         meets the other node filters settings.

#         :arg client: An :class:`elasticsearch.Elasticsearch` client object
#         """
#         mvn_avail = 0
#         # mvn_total = 0
#         mvn_name = None
#         mvn_id = None
#         nodes = self.client.nodes.stats()['nodes']
#         for node_id in nodes:
#             name = nodes[node_id]['name']
#             if self._exclude_node(name):
#                 self.loggit.debug('Node "{0}" excluded by node filters'.format(name))
#                 continue
#             if not self._data_node(node_id):
#                 self.loggit.debug('Node "{0}" is not a data node'.format(name))
#                 continue
#             if not single_data_path(self.client, node_id):
#                 self.loggit.info(
#                     'Node "{0}" has multiple data paths and will not be used for '
#                     'shrink operations.'.format(name))
#                 continue
#             value = nodes[node_id]['fs']['total']['available_in_bytes']
#             if value > mvn_avail:
#                 mvn_name  = name
#                 mvn_id    = node_id
#                 mvn_avail = value
#                 # mvn_total = nodes[node_id]['fs']['total']['total_in_bytes']
#         self.shrink_node_name  = mvn_name
#         self.shrink_node_id    = mvn_id
#         self.shrink_node_avail = mvn_avail
#         # self.shrink_node_total = mvn_total

#     def route_index(self, idx, allocation_type, key, value):
#         bkey = 'index.routing.allocation.{0}.{1}'.format(allocation_type, key)
#         routing = { bkey : value }
#         try:
#             self.client.indices.put_settings(index=idx, body=routing)
#             if self.wait_for_rebalance:
#                 wait_for_it(self.client, 'allocation', wait_interval=self.wait_interval, max_wait=self.max_wait)
#             else:
#                 wait_for_it(self.client, 'relocate', index=idx, wait_interval=self.wait_interval, max_wait=self.max_wait)
#         except Exception as e:
#             self.report_failure(e)

#     def __log_action(self, error_msg, dry_run=False):
#         if not dry_run:
#             raise ActionError(error_msg)
#         else:
#             self.loggit.warn('DRY-RUN: {0}'.format(error_msg))

#     def _block_writes(self, idx):
#         block = { 'index.blocks.write': True }
#         self.client.indices.put_settings(index=idx, body=block)

#     def _unblock_writes(self, idx):
#         unblock = { 'index.blocks.write': False }
#         self.client.indices.put_settings(index=idx, body=unblock)

#     def _check_space(self, idx, dry_run=False):
#         # Disk watermark calculation is already baked into `available_in_bytes`
#         size = index_size(self.client, idx)
#         # avail = self.shrink_node_avail
#         padded = (size * 2) + (32 * 1024)
#         if padded < self.shrink_node_avail:
#             self.loggit.debug('Sufficient space available for 2x the size of index "{0}".  Required: {1}, available: {2}'.format(idx, padded, self.shrink_node_avail))
#         else:
#             error_msg = ('Insufficient space available for 2x the size of index "{0}", shrinking will exceed space available. Required: {1}, available: {2}'.format(idx, padded, self.shrink_node_avail))
#             self.__log_action(error_msg, dry_run)

#     def _check_node(self):
#         if self.shrink_node != 'DETERMINISTIC':
#             if not self.shrink_node_name:
#                 self.qualify_single_node()
#         else:
#             self.most_available_node()
#         # At this point, we should have the three shrink-node identifying
#         # instance variables:
#         # - self.shrink_node_name
#         # - self.shrink_node_id
#         # - self.shrink_node_avail
#         # # - self.shrink_node_total - only if needed in the future

#     def _check_target_exists(self, idx, dry_run=False):
#         target = self._shrink_target(idx)
#         if self.client.indices.exists(target):
#             error_msg = 'Target index "{0}" already exists'.format(target)
#             self.__log_action(error_msg, dry_run)

#     def _check_doc_count(self, idx, dry_run=False):
#         max_docs = 2147483519
#         doc_count = self.client.indices.stats(idx)['indices'][idx]['primaries']['docs']['count']
#         if doc_count > (max_docs * self.number_of_shards):
#             error_msg = ('Too many documents ({0}) to fit in {1} shard(s). Maximum number of docs per shard is {2}'.format(doc_count, self.number_of_shards, max_docs))
#             self.__log_action(error_msg, dry_run)

#     def _check_shard_count(self, idx, src_shards, dry_run=False):
#         if self.number_of_shards >= src_shards:
#             error_msg = ('Target number of shards ({0}) must be less than current number of shards ({1}) in index "{2}"'.format(self.number_of_shards, src_shards, idx))
#             self.__log_action(error_msg, dry_run)

#     def _check_shard_factor(self, idx, src_shards, dry_run=False):
#         # Find the list of factors of src_shards
#         factors = [x for x in range(1,src_shards+1) if src_shards % x == 0]
#         # Pop the last one, because it will be the value of src_shards
#         factors.pop()
#         if not self.number_of_shards in factors:
#             error_msg = (
#                 '"{0}" is not a valid factor of {1} shards.  Valid values are '
#                 '{2}'.format(self.number_of_shards, src_shards, factors)
#             )
#             self.__log_action(error_msg, dry_run)

#     def _check_all_shards(self, idx):
#         shards = self.client.cluster.state(index=idx)['routing_table']['indices'][idx]['shards']
#         found = []
#         for shardnum in shards:
#             for shard_idx in range(0, len(shards[shardnum])):
#                 if shards[shardnum][shard_idx]['node'] == self.shrink_node_id:
#                     found.append({'shard': shardnum, 'primary': shards[shardnum][shard_idx]['primary']})
#         if len(shards) != len(found):
#             self.loggit.debug('Found these shards on node "{0}": {1}'.format(self.shrink_node_name, found))
#             raise ActionError('Unable to shrink index "{0}" as not all shards were found on the designated shrink node ({1}): {2}'.format(idx, self.shrink_node_name, found))

#     def pre_shrink_check(self, idx, dry_run=False):
#         self.loggit.debug('BEGIN PRE_SHRINK_CHECK')
#         self.loggit.debug('Check that target exists')
#         self._check_target_exists(idx, dry_run)
#         self.loggit.debug('Check doc count constraints')
#         self._check_doc_count(idx, dry_run)
#         self.loggit.debug('Check shard count')
#         src_shards = int(self.client.indices.get(idx)[idx]['settings']['index']['number_of_shards'])
#         self._check_shard_count(idx, src_shards, dry_run)
#         self.loggit.debug('Check shard factor')
#         self._check_shard_factor(idx, src_shards, dry_run)
#         self.loggit.debug('Check node availability')
#         self._check_node()
#         self.loggit.debug('Check available disk space')
#         self._check_space(idx, dry_run)
#         self.loggit.debug('FINISH PRE_SHRINK_CHECK')

#     def do_copy_aliases(self, source_idx, target_idx):
#         alias_actions = []
#         aliases = self.client.indices.get_alias(index=source_idx)
#         for alias in aliases[source_idx]['aliases']:
#             self.loggit.debug('alias: {0}'.format(alias))
#             alias_actions.append(
#                 {'remove': {'index': source_idx, 'alias': alias}})
#             alias_actions.append(
#                 {'add': {'index': target_idx, 'alias': alias}})
#         if alias_actions:
#             self.loggit.info('Copy alias actions: {0}'.format(alias_actions))
#             self.client.indices.update_aliases({ 'actions' : alias_actions })

#     def do_dry_run(self):
#         """
#         Show what a regular run would do, but don't actually do it.
#         """
#         self.index_list.filter_closed()
#         self.index_list.empty_list_check()
#         try:
#             index_lists = chunk_index_list(self.index_list.indices)
#             for l in index_lists:
#                 for idx in l: # Shrink can only be done one at a time...
#                     target = self._shrink_target(idx)
#                     self.pre_shrink_check(idx, dry_run=True)
#                     self.loggit.info('DRY-RUN: Moving shards to shrink node: "{0}"'.format(self.shrink_node_name))
#                     self.loggit.info('DRY-RUN: Shrinking index "{0}" to "{1}" with settings: {2}, wait_for_active_shards={3}'.format(idx, target, self.body, self.wait_for_active_shards))
#                     if self.post_allocation:
#                         self.loggit.info('DRY-RUN: Applying post-shrink allocation rule "{0}" to index "{1}"'.format('index.routing.allocation.{0}.{1}:{2}'.format(self.post_allocation['allocation_type'], self.post_allocation['key'], self.post_allocation['value']), target))
#                     if self.copy_aliases:
#                         self.loggit.info('DRY-RUN: Copy source index aliases "{0}"'.format(self.client.indices.get_alias(idx)))
#                         #self.do_copy_aliases(idx, target)
#                     if self.delete_after:
#                         self.loggit.info('DRY-RUN: Deleting source index "{0}"'.format(idx))
#         except Exception as e:
#             self.report_failure(e)

#     def do_action(self):
#         self.index_list.filter_closed()
#         self.index_list.empty_list_check()
#         try:
#             index_lists = chunk_index_list(self.index_list.indices)
#             for l in index_lists:
#                 for idx in l: # Shrink can only be done one at a time...
#                     target = self._shrink_target(idx)
#                     self.loggit.info('Source index: {0} -- Target index: {1}'.format(idx, target))
#                     # Pre-check ensures disk space available for each pass of the loop
#                     self.pre_shrink_check(idx)
#                     # Route the index to the shrink node
#                     self.loggit.info('Moving shards to shrink node: "{0}"'.format(self.shrink_node_name))
#                     self.route_index(idx, 'require', '_name', self.shrink_node_name)
#                     # Ensure a copy of each shard is present
#                     self._check_all_shards(idx)
#                     # Block writes on index
#                     self._block_writes(idx)
#                     # Do final health check
#                     if not health_check(self.client, status='green'):
#                         raise ActionError('Unable to proceed with shrink action. Cluster health is not "green"')
#                     # Do the shrink
#                     self.loggit.info('Shrinking index "{0}" to "{1}" with settings: {2}, wait_for_active_shards={3}'.format(idx, target, self.body, self.wait_for_active_shards))
#                     try:
#                         self.client.indices.shrink(index=idx, target=target, body=self.body, wait_for_active_shards=self.wait_for_active_shards)
#                         # Wait for it to complete
#                         if self.wfc:
#                             self.loggit.debug('Wait for shards to complete allocation for index: {0}'.format(target))
#                             if self.wait_for_rebalance:
#                                 wait_for_it(self.client, 'shrink', wait_interval=self.wait_interval, max_wait=self.max_wait)
#                             else:
#                                 wait_for_it(self.client, 'relocate', index=target, wait_interval=self.wait_interval, max_wait=self.max_wait)
#                     except Exception as e:
#                         if self.client.indices.exists(index=target):
#                             self.loggit.error('Deleting target index "{0}" due to failure to complete shrink'.format(target))
#                             self.client.indices.delete(index=target)
#                         raise ActionError('Unable to shrink index "{0}" -- Error: {1}'.format(idx, e))
#                     self.loggit.info('Index "{0}" successfully shrunk to "{1}"'.format(idx, target))
#                     # Do post-shrink steps
#                     # Unblock writes on index (just in case)
#                     self._unblock_writes(idx)
#                     ## Post-allocation, if enabled
#                     if self.post_allocation:
#                         self.loggit.info('Applying post-shrink allocation rule "{0}" to index "{1}"'.format('index.routing.allocation.{0}.{1}:{2}'.format(self.post_allocation['allocation_type'], self.post_allocation['key'], self.post_allocation['value']), target))
#                         self.route_index(target, self.post_allocation['allocation_type'], self.post_allocation['key'], self.post_allocation['value'])
#                     ## Copy aliases, if flagged
#                     if self.copy_aliases:
#                         self.loggit.info('Copy source index aliases "{0}"'.format(idx))
#                         self.do_copy_aliases(idx, target)
#                     ## Delete, if flagged
#                     if self.delete_after:
#                         self.loggit.info('Deleting source index "{0}"'.format(idx))
#                         self.client.indices.delete(index=idx)
#                     else: # Let's unset the routing we applied here.
#                         self.loggit.info('Unassigning routing for source index: "{0}"'.format(idx))
#                         self.route_index(idx, 'require', '_name', '')

#         except Exception as e:
#             # Just in case it fails after attempting to meet this condition
#             self._unblock_writes(idx)
#             self.report_failure(e)
