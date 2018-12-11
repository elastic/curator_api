"""Index helpers"""
import logging
from curator_api.exceptions import FailedExecution
from curator_api.helpers.client import get_version




def index_size(client, idx):
    """
    Return the sum of all primary and all replica shards `size_in_bytes`
    """
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
    logger = logging.getLogger(__name__)
    try:
        indices = list(
            client.indices.get_settings(
                index='_all', params={'expand_wildcards': 'open,closed'})
            )
        version_number = get_version(client)
        logger.debug(
            'Detected Elasticsearch version '
            '{0}'.format(".".join(map(str, version_number)))
        )
        logger.debug("All indices: {0}".format(indices))
        return indices
    except Exception as err:
        raise FailedExecution('Failed to get indices. Error: {0}'.format(err))
