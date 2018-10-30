from curator_api.exceptions import FailedExecution
import logging

logger = logging.getLogger(__name__)

def index_size(client, idx):
    return client.indices.stats(index=idx)['indices'][idx]['total']['store']['size_in_bytes']


def chunk_index_list(indices):
    """
    This utility chunks very large index lists into 3KB chunks
    It measures the size as a csv string, then converts back into a list
    for the return value.

    :arg indices: A list of indices to act on.
    :rtype: list
    """
    chunks = []
    chunk = ""
    for index in indices:
        if len(chunk) < 3072:
            if not chunk:
                chunk = index
            else:
                chunk += "," + index
        else:
            chunks.append(chunk.split(','))
            chunk = index
    chunks.append(chunk.split(','))
    return chunks


def get_indices(client):
    """
    Get the current list of indices from the cluster.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: list
    """
    try:
        indices = list(
            client.indices.get_settings(
            index='_all', params={'expand_wildcards': 'open,closed'})
        )
        version_number = get_version(client)
        logger.debug(
            'Detected Elasticsearch version '
            '{0}'.format(".".join(map(str,version_number)))
        )
        logger.debug("All indices: {0}".format(indices))
        return indices
    except Exception as e:
        raise FailedExecution('Failed to get indices. Error: {0}'.format(e))


def verify_index_list(test):
    """
    Test if `test` is a proper :class:`curator.indexlist.IndexList` object and
    raise an exception if it is not.

    :arg test: The variable or object to test
    :rtype: None
    """
    # It breaks if this import isn't local to this function
    from ..indexlist import IndexList
    if not isinstance(test, IndexList):
        raise TypeError(
            'Not an IndexList object. Type: {0}.'.format(type(test))
        )
