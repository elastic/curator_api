import logging
from curator_api.exceptions import ActionError, CuratorException, FailedExecution, MissingArgument
from elasticsearch.exceptions import NotFoundError, TransportError

logger = logging.getLogger(__name__)

def check_repo_fs(client, repository=None):
    """
    Test whether all nodes have write access to the repository

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    """
    try:
        nodes = client.snapshot.verify_repository(
            repository=repository)['nodes']
        logger.debug('All nodes can write to the repository')
        logger.debug(
            'Nodes with verified repository access: {0}'.format(nodes))
    except Exception as e:
        try:
            if e.status_code == 404:
                msg = (
                    '--- Repository "{0}" not found. Error: '
                    '{1}, {2}'.format(repository, e.status_code, e.error)
                )
            else:
                msg = (
                    '--- Got a {0} response from Elasticsearch.  '
                    'Error message: {1}'.format(e.status_code, e.error)
                )
        except AttributeError:
            msg = ('--- Error message: {0}'.format(e))
        raise ActionError(
            'Failed to verify all nodes have repository access: '
            '{0}'.format(msg)
        )

def get_repository(client, repository=''):
    """
    Return configuration information for the indicated repository.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    :rtype: dict
    """
    try:
        return client.snapshot.get_repository(repository=repository)
    except (TransportError, NotFoundError) as e:
        raise CuratorException(
            'Unable to get repository {0}.  Response Code: {1}.  Error: {2}.'
            'Check Elasticsearch logs for more information.'.format(
                repository, e.status_code, e.error
            )
        )  

def repository_exists(client, repository=None):
    """
    Verify the existence of a repository

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    :rtype: bool
    """
    if not repository:
        raise MissingArgument('No value for "repository" provided')
    try:
        test_result = get_repository(client, repository)
        if repository in test_result:
            logger.debug("Repository {0} exists.".format(repository))
            return True
        else:
            logger.debug("Repository {0} not found...".format(repository))
            return False
    except Exception as e:
        logger.debug(
            'Unable to find repository "{0}": Error: '
            '{1}'.format(repository, e)
        )
        return False

def create_repo_body(repo_type=None,
                     compress=True, chunk_size=None,
                     max_restore_bytes_per_sec=None,
                     max_snapshot_bytes_per_sec=None,
                     location=None,
                     bucket=None, region=None, base_path=None, access_key=None,
                     secret_key=None, **kwargs):
    """
    Build the 'body' portion for use in creating a repository.

    :arg repo_type: The type of repository (presently only `fs` and `s3`)
    :arg compress: Turn on compression of the snapshot files. Compression is
        applied only to metadata files (index mapping and settings). Data files
        are not compressed. (Default: `True`)
    :arg chunk_size: The chunk size can be specified in bytes or by using size
        value notation, i.e. 1g, 10m, 5k. Defaults to `null` (unlimited chunk
        size).
    :arg max_restore_bytes_per_sec: Throttles per node restore rate. Defaults
        to ``20mb`` per second.
    :arg max_snapshot_bytes_per_sec: Throttles per node snapshot rate. Defaults
        to ``20mb`` per second.
    :arg location: Location of the snapshots. Required.
    :arg bucket: `S3 only.` The name of the bucket to be used for snapshots.
        Required.
    :arg region: `S3 only.` The region where bucket is located. Defaults to
        `US Standard`
    :arg base_path: `S3 only.` Specifies the path within bucket to repository
        data. Defaults to value of ``repositories.s3.base_path`` or to root
        directory if not set.
    :arg access_key: `S3 only.` The access key to use for authentication.
        Defaults to value of ``cloud.aws.access_key``.
    :arg secret_key: `S3 only.` The secret key to use for authentication.
        Defaults to value of ``cloud.aws.secret_key``.

    :returns: A dictionary suitable for creating a repository from the provided
        arguments.
    :rtype: dict
    """
    # This shouldn't happen, but just in case...
    if not repo_type:
        raise MissingArgument('Missing required parameter --repo_type')

    argdict = locals()
    body = {}
    body['type'] = argdict['repo_type']
    body['settings'] = {}
    settingz = [] # Differentiate from module settings
    maybes   = [
                'compress', 'chunk_size',
                'max_restore_bytes_per_sec', 'max_snapshot_bytes_per_sec'
               ]
    s3       = ['bucket', 'region', 'base_path', 'access_key', 'secret_key']

    settingz += [i for i in maybes if argdict[i]]
    # Type 'fs'
    if argdict['repo_type'] == 'fs':
        settingz.append('location')
    # Type 's3'
    if argdict['repo_type'] == 's3':
        settingz += [i for i in s3 if argdict[i]]
    for k in settingz:
        body['settings'][k] = argdict[k]
    return body

def create_repository(client, **kwargs):
    """
    Create repository with repository and body settings

    :arg client: An :class:`elasticsearch.Elasticsearch` client object

    :arg repository: The Elasticsearch snapshot repository to use
    :arg repo_type: The type of repository (presently only `fs` and `s3`)
    :arg compress: Turn on compression of the snapshot files. Compression is
        applied only to metadata files (index mapping and settings). Data files
        are not compressed. (Default: `True`)
    :arg chunk_size: The chunk size can be specified in bytes or by using size
        value notation, i.e. 1g, 10m, 5k. Defaults to `null` (unlimited chunk
        size).
    :arg max_restore_bytes_per_sec: Throttles per node restore rate. Defaults
        to ``20mb`` per second.
    :arg max_snapshot_bytes_per_sec: Throttles per node snapshot rate. Defaults
        to ``20mb`` per second.
    :arg location: Location of the snapshots. Required.
    :arg bucket: `S3 only.` The name of the bucket to be used for snapshots.
        Required.
    :arg region: `S3 only.` The region where bucket is located. Defaults to
        `US Standard`
    :arg base_path: `S3 only.` Specifies the path within bucket to repository
        data. Defaults to value of ``repositories.s3.base_path`` or to root
        directory if not set.
    :arg access_key: `S3 only.` The access key to use for authentication.
        Defaults to value of ``cloud.aws.access_key``.
    :arg secret_key: `S3 only.` The secret key to use for authentication.
        Defaults to value of ``cloud.aws.secret_key``.
    :arg skip_repo_fs_check: Skip verifying the repo after creation.

    :returns: A boolean value indicating success or failure.
    :rtype: bool
    """
    if not 'repository' in kwargs:
        raise MissingArgument('Missing required parameter "repository"')
    else:
        repository = kwargs['repository']
    skip_repo_fs_check = kwargs.pop('skip_repo_fs_check', False)
    params = {'verify': 'false' if skip_repo_fs_check else 'true'}

    try:
        body = create_repo_body(**kwargs)
        logger.debug(
            'Checking if repository {0} already exists...'.format(repository)
        )
        result = repository_exists(client, repository=repository)
        logger.debug("Result = {0}".format(result))
        if not result:
            logger.debug(
                'Repository {0} not in Elasticsearch. Continuing...'.format(
                    repository
                )
            )
            client.snapshot.create_repository(repository=repository, body=body, params=params)
        else:
            raise FailedExecution(
                'Unable to create repository {0}.  A repository with that name '
                'already exists.'.format(repository)
            )
    except TransportError as e:
        raise FailedExecution(
            """
            Unable to create repository {0}.  Response Code: {1}.  Error: {2}.
            Check curator and elasticsearch logs for more information.
            """.format(
                repository, e.status_code, e.error
                )
        )
    logger.debug("Repository {0} creation initiated...".format(repository))
    return True