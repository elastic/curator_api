import logging
from curator.actions.parentclasses import ActionClass
from curator.exceptions import ConfigurationError
from curator.helpers.alias import rollable_alias
from curator.helpers.client import get_version, verify_client_object
from curator.helpers.datemath import parse_date_pattern

class Rollover(ActionClass):
    def __init__(
            self, client, name, conditions, new_index=None, extra_settings=None,
            wait_for_active_shards=1
        ):
        """
        :arg client: An :class:`elasticsearch.Elasticsearch` client object
        :arg name: The name of the single-index-mapped alias to test for
            rollover conditions.
        :new_index: The new index name
        :arg conditions: A dictionary of conditions to test
        :arg extra_settings: Must be either `None`, or a dictionary of settings
            to apply to the new index on rollover. This is used in place of
            `settings` in the Rollover API, mostly because it's already existent
            in other places here in Curator
        :arg wait_for_active_shards: The number of shards expected to be active
            before returning.
        """
        verify_client_object(client)
        self.loggit     = logging.getLogger('curator.actions.rollover')
        if not isinstance(conditions, dict):
            raise ConfigurationError('"conditions" must be a dictionary')
        else:
            self.loggit.debug('"conditions" is {0}'.format(conditions))
        if not isinstance(extra_settings, dict) and extra_settings is not None:
            raise ConfigurationError(
                '"extra_settings" must be a dictionary or None')
        #: Instance variable.
        #: The Elasticsearch Client object
        self.client     = client
        #: Instance variable.
        #: Internal reference to `conditions`
        self.conditions = self._check_max_size(conditions)
        #: Instance variable.
        #: Internal reference to `extra_settings`
        self.settings   = extra_settings
        #: Instance variable.
        #: Internal reference to `new_index`
        self.new_index = parse_date_pattern(new_index) if new_index else new_index
        #: Instance variable.
        #: Internal reference to `wait_for_active_shards`
        self.wait_for_active_shards = wait_for_active_shards

        # Verify that `conditions` and `settings` are good?
        # Verify that `name` is an alias, and is only mapped to one index.
        if rollable_alias(client, name):
            self.name = name
        else:
            raise ValueError(
                    'Unable to perform index rollover with alias '
                    '"{0}". See previous logs for more details.'.format(name)
                )

    def _check_max_size(self, data):
        """
        Ensure that if ``max_size`` is specified, that ``self.client``
        is running 6.1 or higher.
        """
        try:
            if 'max_size' in data['conditions']:
                version = get_version(self.client)
                if version < (6,1,0):
                    raise ConfigurationError(
                        'Your version of elasticsearch ({0}) does not support '
                        'the max_size rollover condition. It is only supported '
                        'in versions 6.1.0 and up.'.format(version)
                    )
        except KeyError:
            self.loggit.debug('data does not contain dict key "conditions"')
        return data

    def body(self):
        """
        Create a body from conditions and settings
        """
        retval = {}
        retval['conditions'] = self.conditions
        if self.settings:
            retval['settings'] = self.settings
        return retval

    def log_result(self, result):
        """
        Log the results based on whether the index rolled over or not
        """
        dryrun_string = ''
        if result['dry_run']:
            dryrun_string = 'DRY-RUN: '
        self.loggit.debug('{0}Result: {1}'.format(dryrun_string, result))
        rollover_string = '{0}Old index {1} rolled over to new index {2}'.format(
            dryrun_string,
            result['old_index'],
            result['new_index']
        )
        # Success is determined by at one condition being True
        success = False
        for k in list(result['conditions'].keys()):
            if result['conditions'][k]:
                success = True
        if result['dry_run'] and success: # log "successful" dry-run
            self.loggit.info(rollover_string)
        elif result['rolled_over']:
            self.loggit.info(rollover_string)
        else:
            self.loggit.info(
                '{0}Rollover conditions not met. Index {0} not rolled over.'.format(
                    dryrun_string,
                    result['old_index'])
            )

    def doit(self, dry_run=False):
        """
        This exists solely to prevent having to have duplicate code in both
        `do_dry_run` and `do_action`
        """
        return self.client.indices.rollover(
            alias=self.name,
            new_index=self.new_index,
            body=self.body(),
            dry_run=dry_run,
            wait_for_active_shards=self.wait_for_active_shards,
        )

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        self.log_result(self.doit(dry_run=True))

    def do_action(self):
        """
        Rollover the index referenced by alias `name`
        """
        self.loggit.info('Performing index rollover')
        try:
            self.log_result(self.doit())
        except Exception as e:
            self.report_failure(e)
