import logging
from curator_api.exceptions import ConfigurationError, FailedExecution

logger = logging.getLogger(__name__)

def node_roles(client, node_id):
    """
    Return the list of roles assigned to the node identified by ``node_id``

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: list
    """
    return client.nodes.info()['nodes'][node_id]['roles']


def single_data_path(client, node_id):
    """
    In order for a shrink to work, it should be on a single filesystem, as 
    shards cannot span filesystems.  Return `True` if the node has a single
    filesystem, and `False` otherwise.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: bool
    """
    return len(client.nodes.stats()['nodes'][node_id]['fs']['data']) == 1 


def name_to_node_id(client, name):
    """
    Return the node_id of the node identified by ``name``

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: str
    """
    stats = client.nodes.stats()
    for node in stats['nodes']:
        if stats['nodes'][node]['name'] == name:
            logger.debug('Found node_id "{0}" for name "{1}".'.format(node, name))
            return node
    logger.error('No node_id found matching name: "{0}"'.format(name))
    return None

def node_id_to_name(client, node_id):
    """
    Return the name of the node identified by ``node_id``

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: str
    """
    stats = client.nodes.stats()
    name = None
    if node_id in stats['nodes']:
        name = stats['nodes'][node_id]['name']
    else:
        logger.error('No node_id found matching: "{0}"'.format(node_id))
    logger.debug('Name associated with node_id "{0}": {1}'.format(node_id, name))
    return name