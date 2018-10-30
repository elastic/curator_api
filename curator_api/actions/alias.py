"""Alias Module"""
import logging
from curator_api.actions.parentclasses import ActionClass
from curator_api.exceptions import ActionError, MissingArgument, NoIndices
from curator_api.helpers.datemath import parse_date_pattern, parse_datemath
from curator_api.helpers.index import verify_index_list

class Alias(ActionClass):
    """Alias Class"""
    def __init__(self, name=None, extra_settings={}, **kwargs):
        """
        Define the Alias object.

        :arg name: The alias name
        :arg extra_settings: Extra settings, including filters and routing. For
            more information see
            https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html
        :type extra_settings: dict, representing the settings.
        """
        if not name:
            raise MissingArgument('No value for "name" provided.')
        #: Instance variable
        #: The strftime parsed version of `name`.
        self.name = parse_date_pattern(name)
        #: The list of actions to perform.  Populated by
        #: :mod:`curator.actions.Alias.add` and
        #: :mod:`curator.actions.Alias.remove`
        self.actions = []
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client = None
        #: Instance variable.
        #: Any extra things to add to the alias, like filters, or routing.
        self.extra_settings = extra_settings
        self.loggit = logging.getLogger('curator.actions.alias')
        #: Instance variable.
        #: Preset default value to `False`.
        self.warn_if_no_indices = False

    def add(self, ilo, warn_if_no_indices=False):
        """
        Create `add` statements for each index in `ilo` for `alias`, then
        append them to `actions`.  Add any `extras` that may be there.

        :arg ilo: A :class:`curator.indexlist.IndexList` object

        """
        verify_index_list(ilo)
        if not self.client:
            self.client = ilo.client
        self.name = parse_datemath(self.client, self.name)
        try:
            ilo.empty_list_check()
        except NoIndices:
            # Add a warning if there are no indices to add, if so set in options
            if warn_if_no_indices:
                self.warn_if_no_indices = True
                self.loggit.warn('No indices found after processing filters. Nothing to add to {0}'.format(self.name))
                return
            else:
                # Re-raise the NoIndices so it will behave as before
                raise NoIndices
        for index in ilo.working_list():
            self.loggit.debug('Adding index {0} to alias {1} with extra settings {2}'.format(index, self.name, self.extra_settings))
            add_dict = { 'add' : { 'index' : index, 'alias': self.name } }
            add_dict['add'].update(self.extra_settings)
            self.actions.append(add_dict)

    def remove(self, ilo, warn_if_no_indices=False):
        """
        Create `remove` statements for each index in `ilo` for `alias`,
        then append them to `actions`.

        :arg ilo: A :class:`curator.indexlist.IndexList` object
        """
        verify_index_list(ilo)
        if not self.client:
            self.client = ilo.client
        self.name = parse_datemath(self.client, self.name)
        try:
            ilo.empty_list_check()
        except NoIndices:
            # Add a warning if there are no indices to add, if so set in options
            if warn_if_no_indices:
                self.warn_if_no_indices = True
                self.loggit.warn('No indices found after processing filters. Nothing to remove from {0}'.format(self.name))
                return
            else:
                # Re-raise the NoIndices so it will behave as before
                raise NoIndices
        aliases = self.client.indices.get_alias()
        for index in ilo.working_list():
            if index in aliases:
                self.loggit.debug('Index {0} in get_aliases output'.format(index))
                # Only remove if the index is associated with the alias
                if self.name in aliases[index]['aliases']:
                    self.loggit.debug('Removing index {0} from alias {1}'.format(index, self.name))
                    self.actions.append(
                        { 'remove' : { 'index' : index, 'alias': self.name } })
                else:
                    self.loggit.debug('Can not remove: Index {0} is not associated with alias {1}'.format(index, self.name))

    def body(self):
        """
        Return a `body` string suitable for use with the `update_aliases` API
        call.
        """
        if not self.actions:
            if not self.warn_if_no_indices:
                raise ActionError('No "add" or "remove" operations')
            else:
                raise NoIndices('No "adds" or "removes" found.  Taking no action')
        self.loggit.debug('Alias actions: {0}'.format(self.actions))

        return { 'actions' : self.actions }

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        for item in self.body()['actions']:
            job = list(item.keys())[0]
            index = item[job]['index']
            alias = item[job]['alias']
            # We want our log to look clever, so if job is "remove", strip the
            # 'e' so "remove" can become "removing".  "adding" works already.
            self.loggit.info(
                'DRY-RUN: alias: {0}ing index "{1}" {2} alias "{3}"'.format(
                    job.rstrip('e'),
                    index,
                    'to' if job is 'add' else 'from',
                    alias
                )
            )

    def do_action(self):
        """
        Run the API call `update_aliases` with the results of `body()`
        """
        self.loggit.info('Updating aliases...')
        self.loggit.info('Alias actions: {0}'.format(self.body()))
        try:
            self.client.indices.update_aliases(body=self.body())
        except Exception as e:
            self.report_failure(e)
