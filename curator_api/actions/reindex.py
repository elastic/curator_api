import logging
from copy import deepcopy
from curator.actions.parentclasses import ActionClass
from curator.exceptions import ConfigurationError, NoIndices, FailedExecution
from curator.helpers.client import get_client, get_version
from curator.helpers.index import chunk_index_list, verify_index_list
from curator.helpers.utils import ensure_list, to_csv
from curator.helpers.waiting import wait_for_it

class Reindex(ActionClass):
    def __init__(self, ilo, request_body, refresh=True,
        requests_per_second=-1, slices=1, timeout=60, wait_for_active_shards=1,
        wait_for_completion=True, max_wait=-1, wait_interval=9,
        remote_url_prefix=None, remote_ssl_no_validate=None,
        remote_certificate=None, remote_client_cert=None,
        remote_client_key=None, remote_aws_key=None, remote_aws_secret_key=None,
        remote_aws_region=None, remote_filters={}, migration_prefix='',
        migration_suffix=''):
        """
        :arg ilo: A :class:`curator.indexlist.IndexList` object
        :arg request_body: The body to send to
            :py:meth:`elasticsearch.Elasticsearch.reindex`, which must be complete and
            usable, as Curator will do no vetting of the request_body. If it
            fails to function, Curator will return an exception.
        :arg refresh: Whether to refresh the entire target index after the
            operation is complete. (default: `True`)
        :type refresh: bool
        :arg requests_per_second: The throttle to set on this request in
            sub-requests per second. ``-1`` means set no throttle as does
            ``unlimited`` which is the only non-float this accepts. (default:
            ``-1``)
        :arg slices: The number of slices this task  should be divided into. 1
            means the task will not be sliced into subtasks. (default: ``1``)
        :arg timeout: The length in seconds each individual bulk request should
            wait for shards that are unavailable. (default: ``60``)
        :arg wait_for_active_shards: Sets the number of shard copies that must
            be active before proceeding with the reindex operation. (default:
            ``1``) means the primary shard only. Set to ``all`` for all shard
            copies, otherwise set to any non-negative value less than or equal
            to the total number of copies for the shard (number of replicas + 1)
        :arg wait_for_completion: Wait (or not) for the operation
            to complete before returning.  (default: `True`)
        :type wait_for_completion: bool
        :arg wait_interval: How long in seconds to wait between checks for
            completion.
        :arg max_wait: Maximum number of seconds to `wait_for_completion`
        :arg remote_url_prefix: `Optional` url prefix, if needed to reach the
            Elasticsearch API (i.e., it's not at the root level)
        :type remote_url_prefix: str
        :arg remote_ssl_no_validate: If `True`, do not validate the certificate
            chain.  This is an insecure option and you will see warnings in the
            log output.
        :type remote_ssl_no_validate: bool
        :arg remote_certificate: Path to SSL/TLS certificate
        :arg remote_client_cert: Path to SSL/TLS client certificate (public key)
        :arg remote_client_key: Path to SSL/TLS private key
        :arg remote_aws_key: AWS IAM Access Key (Only used if the
            :mod:`requests-aws4auth` python module is installed)
        :arg remote_aws_secret_key: AWS IAM Secret Access Key (Only used if the
            :mod:`requests-aws4auth` python module is installed)
        :arg remote_aws_region: AWS Region (Only used if the
            :mod:`requests-aws4auth` python module is installed)
        :arg remote_filters: Apply these filters to the remote client for
            remote index selection.
        :arg migration_prefix: When migrating, prepend this value to the index
            name.
        :arg migration_suffix: When migrating, append this value to the index
            name.
        """
        self.loggit = logging.getLogger('curator.actions.reindex')
        verify_index_list(ilo)
        # Normally, we'd check for an empty list here.  But since we can reindex
        # from remote, we might just be starting with an empty one.
        if not isinstance(request_body, dict):
            raise ConfigurationError('"request_body" is not of type dictionary')
        #: Instance variable.
        #: Internal reference to `request_body`
        self.body = request_body
        self.loggit.debug('REQUEST_BODY = {0}'.format(request_body))
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client = ilo.client
        #: Instance variable.
        #: Internal reference to `ilo`
        self.index_list = ilo
        #: Instance variable.
        #: Internal reference to `refresh`
        self.refresh = refresh
        #: Instance variable.
        #: Internal reference to `requests_per_second`
        self.requests_per_second = requests_per_second
        #: Instance variable.
        #: Internal reference to `slices`
        self.slices = slices
        #: Instance variable.
        #: Internal reference to `timeout`, and add "s" for seconds.
        self.timeout = '{0}s'.format(timeout)
        #: Instance variable.
        #: Internal reference to `wait_for_active_shards`
        self.wait_for_active_shards = wait_for_active_shards
        #: Instance variable.
        #: Internal reference to `wait_for_completion`
        self.wfc = wait_for_completion
        #: Instance variable
        #: How many seconds to wait between checks for completion.
        self.wait_interval = wait_interval
        #: Instance variable.
        #: How long in seconds to `wait_for_completion` before returning with an
        #: exception. A value of -1 means wait forever.
        self.max_wait   = max_wait
        #: Instance variable.
        #: Internal reference to `migration_prefix`
        self.mpfx = migration_prefix
        #: Instance variable.
        #: Internal reference to `migration_suffix`
        self.msfx = migration_suffix

        # This is for error logging later...
        self.remote = False
        if 'remote' in self.body['source']:
            self.remote = True

        self.migration = False
        if self.body['dest']['index'] == 'MIGRATION':
            self.migration = True

        if self.migration:
            if not self.remote and not self.mpfx and not self.msfx:
                raise ConfigurationError(
                    'MIGRATION can only be used locally with one or both of '
                    'migration_prefix or migration_suffix.'
                )

        # REINDEX_SELECTION is the designated token.  If you use this for the
        # source "index," it will be replaced with the list of indices from the
        # provided 'ilo' (index list object).
        if self.body['source']['index'] == 'REINDEX_SELECTION' \
                and not self.remote:
            self.body['source']['index'] = self.index_list.indices

        # Remote section
        elif self.remote:
            self.loggit.debug('Remote reindex request detected')
            if 'host' not in self.body['source']['remote']:
                raise ConfigurationError('Missing remote "host"')
            rclient_info = {}
            for k in ['host', 'username', 'password']:
                rclient_info[k] = self.body['source']['remote'][k] \
                    if k in self.body['source']['remote'] else None
            rhost = rclient_info['host']
            try:
                # Save these for logging later
                a = rhost.split(':')
                self.remote_port = a[2]
                self.remote_host = a[1][2:]
            except Exception as e:
                raise ConfigurationError(
                    'Host must be in the form [scheme]://[host]:[port] but '
                    'was [{0}]'.format(rhost)
                )
            rhttp_auth = '{0}:{1}'.format(
                    rclient_info['username'],rclient_info['password']) \
                if (rclient_info['username'] and rclient_info['password']) \
                    else None
            if rhost[:5] == 'http:':
                use_ssl = False
            elif rhost[:5] == 'https':
                use_ssl = True
            else:
                raise ConfigurationError(
                    'Host must be in URL format. You provided: '
                    '{0}'.format(rclient_info['host'])
                )

            # Let's set a decent remote timeout for initially reading
            # the indices on the other side, and collecting their metadata
            remote_timeout = 180

            # The rest only applies if using filters for remote indices
            if self.body['source']['index'] == 'REINDEX_SELECTION':
                self.loggit.debug('Filtering indices from remote')
                from curator.indexlist import IndexList
                self.loggit.debug('Remote client args: '
                    'host={0} '
                    'http_auth={1} '
                    'url_prefix={2} '
                    'use_ssl={3} '
                    'ssl_no_validate={4} '
                    'certificate={5} '
                    'client_cert={6} '
                    'client_key={7} '
                    'aws_key={8} '
                    'aws_secret_key={9} '
                    'aws_region={10} '
                    'timeout={11} '
                    'skip_version_test=True'.format(
                        rhost,
                        rhttp_auth,
                        remote_url_prefix,
                        use_ssl,
                        remote_ssl_no_validate,
                        remote_certificate,
                        remote_client_cert,
                        remote_client_key,
                        remote_aws_key,
                        remote_aws_secret_key,
                        remote_aws_region,
                        remote_timeout
                    )
                )

                try: # let's try to build a remote connection with these!
                    rclient = get_client(
                        host=rhost,
                        http_auth=rhttp_auth,
                        url_prefix=remote_url_prefix,
                        use_ssl=use_ssl,
                        ssl_no_validate=remote_ssl_no_validate,
                        certificate=remote_certificate,
                        client_cert=remote_client_cert,
                        client_key=remote_client_key,
                        aws_key=remote_aws_key,
                        aws_secret_key=remote_aws_secret_key,
                        aws_region=remote_aws_region,
                        skip_version_test=True,
                        timeout=remote_timeout
                    )
                except Exception as e:
                    self.loggit.error(
                        'Unable to establish connection to remote Elasticsearch'
                        ' with provided credentials/certificates/settings.'
                    )
                    self.report_failure(e)
                try:
                    rio = IndexList(rclient)
                    rio.iterate_filters({'filters': remote_filters})
                    try:
                        rio.empty_list_check()
                    except NoIndices:
                        raise FailedExecution(
                            'No actionable remote indices selected after '
                            'applying filters.'
                        )
                    self.body['source']['index'] = rio.indices
                except Exception as e:
                    self.loggit.error(
                        'Unable to get/filter list of remote indices.'
                    )
                    self.report_failure(e)

        self.loggit.debug(
            'Reindexing indices: {0}'.format(self.body['source']['index']))

    def _get_request_body(self, source, dest):
        body = deepcopy(self.body)
        body['source']['index'] = source
        body['dest']['index'] = dest
        return body

    def _get_reindex_args(self, source, dest):
        # Always set wait_for_completion to False. Let 'wait_for_it' do its
        # thing if wait_for_completion is set to True. Report the task_id
        # either way.
        reindex_args = {
            'body':self._get_request_body(source, dest), 'refresh':self.refresh,
            'requests_per_second': self.requests_per_second,
            'timeout': self.timeout,
            'wait_for_active_shards': self.wait_for_active_shards,
            'wait_for_completion': False,
            'slices': self.slices
        }
        version = get_version(self.client)
        if version < (5,1,0):
            self.loggit.info(
                'Your version of elasticsearch ({0}) does not support '
                'sliced scroll for reindex, so that setting will not be '
                'used'.format(version)
            )
            del reindex_args['slices']
        return reindex_args

    def _post_run_quick_check(self, index_name):
        # Verify the destination index is there after the fact
        index_exists = self.client.indices.exists(index=index_name)
        alias_instead = self.client.indices.exists_alias(name=index_name)
        if not index_exists and not alias_instead:
            self.loggit.error(
                'The index described as "{0}" was not found after the reindex '
                'operation. Check Elasticsearch logs for more '
                'information.'.format(index_name)
            )
            if self.remote:
                self.loggit.error(
                    'Did you forget to add "reindex.remote.whitelist: '
                    '{0}:{1}" to the elasticsearch.yml file on the '
                    '"dest" node?'.format(
                        self.remote_host, self.remote_port
                    )
                )
            raise FailedExecution(
                'Reindex failed. The index or alias identified by "{0}" was '
                'not found.'.format(index_name)
            )

    def sources(self):
        # Generator for sources & dests
        dest = self.body['dest']['index']
        source_list = ensure_list(self.body['source']['index'])
        self.loggit.debug('source_list: {0}'.format(source_list))
        if source_list == []: # Empty list
            raise ConfigurationError(
                'Source index must be list of actual indices. '
                'It must not be an empty list.'
            )
        if not self.migration:
            yield self.body['source']['index'], dest

        # Loop over all sources (default will only be one)
        else:
            for source in source_list:
                if self.migration:
                    dest = self.mpfx + source + self.msfx
                yield source, dest

    def show_run_args(self, source, dest):
        """
        Show what will run
        """

        return ('request body: {0} with arguments: '
            'refresh={1} '
            'requests_per_second={2} '
            'slices={3} '
            'timeout={4} '
            'wait_for_active_shards={5} '
            'wait_for_completion={6}'.format(
                self._get_request_body(source, dest),
                self.refresh,
                self.requests_per_second,
                self.slices,
                self.timeout,
                self.wait_for_active_shards,
                self.wfc
            )
        )

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        for source, dest in self.sources():
            self.loggit.info(
                'DRY-RUN: REINDEX: {0}'.format(self.show_run_args(source, dest))
            )

    def do_action(self):
        """
        Execute :py:meth:`elasticsearch.Elasticsearch.reindex` operation with the
        provided request_body and arguments.
        """
        try:
            # Loop over all sources (default will only be one)
            for source, dest in self.sources():
                self.loggit.info('Commencing reindex operation')
                self.loggit.debug(
                    'REINDEX: {0}'.format(self.show_run_args(source, dest)))
                response = self.client.reindex(
                                **self._get_reindex_args(source, dest))

                self.loggit.debug('TASK ID = {0}'.format(response['task']))
                if self.wfc:
                    wait_for_it(
                        self.client, 'reindex', task_id=response['task'],
                        wait_interval=self.wait_interval, max_wait=self.max_wait
                    )
                    self._post_run_quick_check(dest)

                else:
                    self.loggit.warn(
                        '"wait_for_completion" set to {0}.  Remember '
                        'to check task_id "{1}" for successful completion '
                        'manually.'.format(self.wfc, response['task'])
                    )
        except Exception as e:
            self.report_failure(e)
