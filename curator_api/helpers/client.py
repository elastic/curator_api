"""Client helpers"""

def get_version(client):
    """
    Return the ES version number as a tuple.
    Omits trailing tags like -dev, or Beta

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: tuple
    """
    version = client.info()['version']['number']
    version = version.split('-')[0]
    if len(version.split('.')) > 3:
        version = version.split('.')[:-1]
    else:
       version = version.split('.')
    return tuple(map(int, version))
