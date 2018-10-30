import logging
import time
from curator_api.exceptions import ActionTimeout, ConfigurationError, CuratorException, MissingArgument
from curator_api.helpers.utils import to_csv
from datetime import datetime

logger = logging.getLogger(__name__)

def health_check(client, **kwargs):
    """
    This function calls client.cluster.health and, based on the args provided,
    will return `True` or `False` depending on whether that particular keyword 
    appears in the output, and has the expected value.
    If multiple keys are provided, all must match for a `True` response. 

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    """
    logger.debug('KWARGS= "{0}"'.format(kwargs))
    klist = list(kwargs.keys())
    if len(klist) < 1:
        raise MissingArgument('Must provide at least one keyword argument')
    hc_data = client.cluster.health()
    response = True
    
    for k in klist:
        # First, verify that all kwargs are in the list
        if not k in list(hc_data.keys()):
            raise ConfigurationError('Key "{0}" not in cluster health output')
        if not hc_data[k] == kwargs[k]:
            logger.debug(
                'NO MATCH: Value for key "{0}", health check data: '
                '{1}'.format(kwargs[k], hc_data[k])
            )
            response = False
        else:
            logger.debug(
                'MATCH: Value for key "{0}", health check data: '
                '{1}'.format(kwargs[k], hc_data[k])
            )
    if response:
        logger.info('Health Check for all provided keys passed.')   
    return response

def relocate_check(client, index):
    """
    This function calls client.cluster.state with a given index to check if
    all of the shards for that index are in the STARTED state. It will
    return `True` if all shards both primary and replica are in the STARTED
    state, and it will return `False` if any primary or replica shard is in
    a different state.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg index: The index to check the index shards state.
    """
    shard_state_data = client.cluster.state(index=index)['routing_table']['indices'][index]['shards']

    finished_state = all(all(shard['state']=="STARTED" for shard in shards) for shards in shard_state_data.values())
    if finished_state:
        logger.info('Relocate Check for index: "{0}" has passed.'.format(index))
    return finished_state

def snapshot_check(client, snapshot=None, repository=None):
    """
    This function calls `client.snapshot.get` and tests to see whether the 
    snapshot is complete, and if so, with what status.  It will log errors
    according to the result. If the snapshot is still `IN_PROGRESS`, it will 
    return `False`.  `SUCCESS` will be an `INFO` level message, `PARTIAL` nets
    a `WARNING` message, `FAILED` is an `ERROR`, message, and all others will be
    a `WARNING` level message.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg snapshot: The name of the snapshot.
    :arg repository: The Elasticsearch snapshot repository to use
    """
    try:
        state = client.snapshot.get(
            repository=repository, snapshot=snapshot)['snapshots'][0]['state']
    except Exception as e:
        raise CuratorException(
            'Unable to obtain information for snapshot "{0}" in repository '
            '"{1}". Error: {2}'.format(snapshot, repository, e)
        )
    logger.debug('Snapshot state = {0}'.format(state))
    if state == 'IN_PROGRESS':
        logger.info('Snapshot {0} still in progress.'.format(snapshot))
        return False
    elif state == 'SUCCESS':
        logger.info(
            'Snapshot {0} successfully completed.'.format(snapshot))
    elif state == 'PARTIAL':
        logger.warn(
            'Snapshot {0} completed with state PARTIAL.'.format(snapshot))
    elif state == 'FAILED':
        logger.error(
            'Snapshot {0} completed with state FAILED.'.format(snapshot))
    else:
        logger.warn(
            'Snapshot {0} completed with state: {0}'.format(snapshot))
    return True


def restore_check(client, index_list):
    """
    This function calls client.indices.recovery with the list of indices to 
    check for complete recovery.  It will return `True` if recovery of those 
    indices is complete, and `False` otherwise.  It is designed to fail fast:
    if a single shard is encountered that is still recovering (not in `DONE`
    stage), it will immediately return `False`, rather than complete iterating
    over the rest of the response.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object 
    :arg index_list: The list of indices to verify having been restored.
    """
    try:
        response = client.indices.recovery(index=to_csv(index_list), human=True)
    except Exception as e:
        raise CuratorException(
            'Unable to obtain recovery information for specified indices. '
            'Error: {0}'.format(e)
        )
    # This should address #962, where perhaps the cluster state hasn't yet
    # had a chance to add a _recovery state yet, so it comes back empty.
    if response == {}:
        logger.info('_recovery returned an empty response. Trying again.')
        return False
    # Fixes added in #989
    logger.info('Provided indices: {0}'.format(index_list))
    logger.info('Found indices: {0}'.format(list(response.keys())))
    for index in response:
        for shard in range(0, len(response[index]['shards'])):
            # Apparently `is not` is not always `!=`.  Unsure why, will
            # research later.  Using != fixes #966
            if response[index]['shards'][shard]['stage'] != 'DONE':
                logger.info(
                    'Index "{0}" is still in stage "{1}"'.format(
                        index, response[index]['shards'][shard]['stage']
                    )
                )
                return False
    # If we've gotten here, all of the indices have recovered
    return True


def task_check(client, task_id=None):
    """
    This function calls client.tasks.get with the provided `task_id`.  If the
    task data contains ``'completed': True``, then it will return `True` 
    If the task is not completed, it will log some information about the task
    and return `False`

    :arg client: An :class:`elasticsearch.Elasticsearch` client object    
    :arg task_id: A task_id which ostensibly matches a task searchable in the
        tasks API.
    """
    try:
        task_data = client.tasks.get(task_id=task_id)
    except Exception as e:
        raise CuratorException(
            'Unable to obtain task information for task_id "{0}". Exception '
            '{1}'.format(task_id, e)
        )
    task = task_data['task']
    completed = task_data['completed']
    running_time = 0.000000001 * task['running_time_in_nanos']
    logger.debug('running_time_in_nanos = {0}'.format(running_time))
    descr = task['description']

    if completed:
        completion_time = ((running_time * 1000) + task['start_time_in_millis'])
        time_string = time.strftime(
            '%Y-%m-%dT%H:%M:%SZ', time.localtime(completion_time/1000)
        )
        logger.info('Task "{0}" completed at {1}.'.format(descr, time_string))
        return True
    else:
        # Log the task status here.
        logger.debug('Full Task Data: {0}'.format(task_data))
        logger.info(
            'Task "{0}" with task_id "{1}" has been running for '
            '{2} seconds'.format(descr, task_id, running_time))
        return False


def wait_for_it(
        client, action, task_id=None, snapshot=None, repository=None,
        index=None, index_list=None, wait_interval=9, max_wait=-1
    ):
    """
    This function becomes one place to do all wait_for_completion type behaviors

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg action: The action name that will identify how to wait
    :arg task_id: If the action provided a task_id, this is where it must be
        declared.
    :arg snapshot: The name of the snapshot.
    :arg repository: The Elasticsearch snapshot repository to use
    :arg wait_interval: How frequently the specified "wait" behavior will be
        polled to check for completion.
    :arg max_wait: Number of seconds will the "wait" behavior persist 
        before giving up and raising an Exception.  The default is -1, meaning
        it will try forever.
    """
    action_map = {
        'allocation':{
            'function': health_check,
            'args': {'relocating_shards':0},
        },
        'replicas':{
            'function': health_check,
            'args': {'status':'green'},
        },
        'cluster_routing':{
            'function': health_check,
            'args': {'relocating_shards':0},
        },
        'snapshot':{
            'function':snapshot_check,
            'args':{'snapshot':snapshot, 'repository':repository},
        },
        'restore':{
            'function':restore_check,
            'args':{'index_list':index_list},
        },
        'reindex':{
            'function':task_check,
            'args':{'task_id':task_id},
        },
        'shrink':{
            'function': health_check,
            'args': {'status':'green'},
        },
        'relocate':{
            'function': relocate_check,
            'args': {'index':index}
        },
    }
    wait_actions = list(action_map.keys())

    if action not in wait_actions:
        raise ConfigurationError(
            '"action" must be one of {0}'.format(wait_actions)
        )
    if action == 'reindex' and task_id == None:
        raise MissingArgument(
            'A task_id must accompany "action" {0}'.format(action)
        )
    if action == 'snapshot' and ((snapshot == None) or (repository == None)):
        raise MissingArgument(
            'A snapshot and repository must accompany "action" {0}. snapshot: '
            '{1}, repository: {2}'.format(action, snapshot, repository)
        )
    if action == 'restore' and index_list == None:
        raise MissingArgument(
            'An index_list must accompany "action" {0}'.format(action)
        )
    elif action == 'reindex':
        try:
            _ = client.tasks.get(task_id=task_id)
        except Exception as e:
            # This exception should only exist in API usage. It should never
            # occur in regular Curator usage.
            raise CuratorException(
                'Unable to find task_id {0}. Exception: {1}'.format(task_id, e)
            )

    # Now with this mapped, we can perform the wait as indicated.
    start_time = datetime.now()
    result = False
    while True:
        elapsed = int((datetime.now() - start_time).total_seconds())
        logger.debug('Elapsed time: {0} seconds'.format(elapsed))
        response = action_map[action]['function'](
            client, **action_map[action]['args'])
        logger.debug('Response: {0}'.format(response))
        # Success
        if response:
            logger.debug(
                'Action "{0}" finished executing (may or may not have been '
                'successful)'.format(action))
            result = True
            break
        # Not success, and reached maximum wait (if defined)
        elif (max_wait != -1) and (elapsed >= max_wait):
            logger.error(
                'Unable to complete action "{0}" within max_wait ({1}) '
                'seconds.'.format(action, max_wait)
            )
            break
        # Not success, so we wait.
        else:
            logger.debug(
                'Action "{0}" not yet complete, {1} total seconds elapsed. '
                'Waiting {2} seconds before checking '
                'again.'.format(action, elapsed, wait_interval))
            time.sleep(wait_interval)

    logger.debug('Result: {0}'.format(result))
    if result == False:
        raise ActionTimeout(
            'Action "{0}" failed to complete in the max_wait period of '
            '{1} seconds'.format(action, max_wait)
        )