import logging
from curator_api.exceptions import ActionError, CuratorException, FailedExecution, MissingArgument
from curator_api.helpers.notification import report_failure
from curator_api.helpers.utils import to_csv
from elasticsearch.exceptions import NotFoundError, TransportError
from time import sleep

logger = logging.getLogger(__name__)

def get_snapshot(client, repository=None, snapshot=''):
    """
    Return information about a snapshot (or a comma-separated list of snapshots)
    If no snapshot specified, it will return all snapshots.  If none exist, an
    empty dictionary will be returned.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    :arg snapshot: The snapshot name, or a comma-separated list of snapshots
    :rtype: dict
    """
    if not repository:
        raise MissingArgument('No value for "repository" provided')
    snapname = '_all' if snapshot == '' else snapshot
    try:
        return client.snapshot.get(repository=repository, snapshot=snapname)
    except (TransportError, NotFoundError) as e:
        raise FailedExecution(
            'Unable to get information about snapshot {0} from repository: '
            '{1}.  Error: {2}'.format(snapname, repository, e)
        )

def get_snapshot_data(client, repository=None):
    """
    Get ``_all`` snapshots from repository and return a list.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    :rtype: list
    """
    if not repository:
        raise MissingArgument('No value for "repository" provided')
    try:
        return client.snapshot.get(
            repository=repository, snapshot="_all")['snapshots']
    except (TransportError, NotFoundError) as e:
        raise FailedExecution(
            'Unable to get snapshot information from repository: {0}.  '
            'Error: {1}'.format(repository, e)
        )

def snapshot_in_progress(client, repository=None, snapshot=None):
    """
    Determine whether the provided snapshot in `repository` is ``IN_PROGRESS``.
    If no value is provided for `snapshot`, then check all of them.
    Return `snapshot` if it is found to be in progress, or `False`

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    :arg snapshot: The snapshot name
    """
    snapname = '_all' if not snapshot else snapshot
    allsnaps = get_snapshot(
                    client, repository=repository, snapshot=snapname)['snapshots']
    inprogress = (
        [snap['snapshot'] for snap in allsnaps if 'state' in snap.keys() \
            and snap['state'] == 'IN_PROGRESS']
    )
    if snapshot:
        return snapshot if snapshot in inprogress else False
    else:
        if len(inprogress) == 0:
            return False
        elif len(inprogress) == 1:
            return inprogress[0]
        else: # This should not be possible
            raise CuratorException(
                'More than 1 snapshot in progress: {0}'.format(inprogress)
            )

def verify_snapshot_list(test):
    """
    Test if `test` is a proper :class:`curator.snapshotlist.SnapshotList`
    object and raise an exception if it is not.

    :arg test: The variable or object to test
    :rtype: None
    """
    # It breaks if this import isn't local to this function
    from ..snapshotlist import SnapshotList
    if not isinstance(test, SnapshotList):    
        raise TypeError(
            'Not an SnapshotList object. Type: {0}.'.format(type(test))
        )

def find_snapshot_tasks(client):
    """
    Check if there is snapshot activity in the Tasks API.
    Return `True` if activity is found, or `False`

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: bool
    """
    retval = False
    tasklist = client.tasks.get()
    for node in tasklist['nodes']:
        for task in tasklist['nodes'][node]['tasks']:
            activity = tasklist['nodes'][node]['tasks'][task]['action']
            if 'snapshot' in activity:
                logger.debug('Snapshot activity detected: {0}'.format(activity))
                retval = True
    return retval


def safe_to_snap(client, repository=None, retry_interval=120, retry_count=3):
    """
    Ensure there are no snapshots in progress.  Pause and retry accordingly

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    :arg retry_interval: Number of seconds to delay betwen retries. Default:
        120 (seconds)
    :arg retry_count: Number of attempts to make. Default: 3
    :rtype: bool
    """
    if not repository:
        raise MissingArgument('No value for "repository" provided')
    for count in range(1, retry_count+1):
        in_progress = snapshot_in_progress(
            client, repository=repository
        )
        ongoing_task = find_snapshot_tasks(client)
        if in_progress or ongoing_task:
            if in_progress:
                logger.info(
                    'Snapshot already in progress: {0}'.format(in_progress))
            elif ongoing_task:
                logger.info('Snapshot activity detected in Tasks API')
            logger.info(
                'Pausing {0} seconds before retrying...'.format(retry_interval))
            sleep(retry_interval)
            logger.info('Retry {0} of {1}'.format(count, retry_count))
        else:
            return True
    return False


def snapshot_running(client):
    """
    Return `True` if a snapshot is in progress, and `False` if not

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: bool
    """
    try:
        status = client.snapshot.status()['snapshots']
    except Exception as e:
        report_failure(e)
    # We will only accept a positively identified False.  Anything else is
    # suspect.
    return False if status == [] else True      



def create_snapshot_body(indices, ignore_unavailable=False,
                         include_global_state=True, partial=False):
    """
    Create the request body for creating a snapshot from the provided
    arguments.

    :arg indices: A single index, or list of indices to snapshot.
    :arg ignore_unavailable: Ignore unavailable shards/indices. (default:
        `False`)
    :type ignore_unavailable: bool
    :arg include_global_state: Store cluster global state with snapshot.
        (default: `True`)
    :type include_global_state: bool
    :arg partial: Do not fail if primary shard is unavailable. (default:
        `False`)
    :type partial: bool
    :rtype: dict
    """
    if not indices:
        logger.error('No indices provided.')
        return False
    body = {
        "ignore_unavailable": ignore_unavailable,
        "include_global_state": include_global_state,
        "partial": partial,
    }
    if indices == '_all':
        body["indices"] = indices
    else:
        body["indices"] = to_csv(indices)
    return body