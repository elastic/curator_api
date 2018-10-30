import logging
from curator_api.defaults.settings import index_filtertypes, snapshot_actions, snapshot_filtertypes
from curator_api.exceptions import ConfigurationError, CuratorException
from curator_api.helpers.utils import prune_nones
from curator_api.validators import SchemaCheck, actions, filters, options
from voluptuous import Schema

logger = logging.getLogger(__name__)

EXCLUDE_FROM_SINGLETONS = [
    'ignore_empty_list', 'timeout_override',
    'continue_if_exception', 'disable_action'
]

def validate_filters(action, filters):
    """
    Validate that the filters are appropriate for the action type, e.g. no
    index filters applied to a snapshot list.

    :arg action: An action name
    :arg filters: A list of filters to test.
    """
    # Define which set of filtertypes to use for testing
    if action in snapshot_actions():
        filtertypes = snapshot_filtertypes()
    else:
        filtertypes = index_filtertypes()
    for f in filters:
        if f['filtertype'] not in filtertypes:
            raise ConfigurationError(
                '"{0}" filtertype is not compatible with action "{1}"'.format(
                    f['filtertype'],
                    action
                )
            )
    # If we get to this point, we're still valid.  Return the original list
    return filters

def validate_actions(data):
    """
    Validate an Action configuration dictionary, as imported from actions.yml,
    for example.

    The method returns a validated and sanitized configuration dictionary.

    :arg data: The configuration dictionary
    :rtype: dict
    """
    # data is the ENTIRE schema...
    clean_config = { }
    # Let's break it down into smaller chunks...
    # First, let's make sure it has "actions" as a key, with a subdictionary
    root = SchemaCheck(data, actions.root(), 'Actions File', 'root').result()
    # We've passed the first step.  Now let's iterate over the actions...
    for action_id in root['actions']:
        # Now, let's ensure that the basic action structure is correct, with
        # the proper possibilities for 'action'
        action_dict = root['actions'][action_id]
        loc = 'Action ID "{0}"'.format(action_id)
        valid_structure = SchemaCheck(
            action_dict,
            actions.structure(action_dict, loc),
            'structure',
            loc
        ).result()
        # With the basic structure validated, now we extract the action name
        current_action = valid_structure['action']
        # And let's update the location with the action.
        loc = 'Action ID "{0}", action "{1}"'.format(
            action_id, current_action)
        clean_options = SchemaCheck(
            prune_nones(valid_structure['options']),
            options.get_schema(current_action),
            'options',
            loc
        ).result()
        clean_config[action_id] = {
            'action' : current_action,
            'description' : valid_structure['description'],
            'options' : clean_options,
        }
        if current_action == 'alias':
            add_remove = {}
            for k in ['add', 'remove']:
                if k in valid_structure:
                    current_filters = SchemaCheck(
                        valid_structure[k]['filters'],
                        Schema(filters.Filters(current_action, location=loc)),
                        '"{0}" filters'.format(k),
                        '{0}, "filters"'.format(loc)
                    ).result()
                    add_remove.update(
                        {
                            k: {
                                'filters' : SchemaCheck(
                                        current_filters,
                                        Schema(
                                            filters.Filters(
                                                current_action,
                                                location=loc
                                            )
                                        ),
                                        'filters',
                                        '{0}, "{1}", "filters"'.format(loc, k)
                                    ).result()
                                }
                        }
                    )
            # Add/Remove here
            clean_config[action_id].update(add_remove)
        elif current_action in ['cluster_routing', 'create_index', 'rollover']:
            # neither cluster_routing nor create_index should have filters
            pass
        else: # Filters key only appears in non-alias actions
            valid_filters = SchemaCheck(
                valid_structure['filters'],
                Schema(filters.Filters(current_action, location=loc)),
                'filters',
                '{0}, "filters"'.format(loc)
            ).result()
            clean_filters = validate_filters(current_action, valid_filters)
            clean_config[action_id].update({'filters' : clean_filters})
        # This is a special case for remote reindex
        if current_action == 'reindex':
            # Check only if populated with something.
            if 'remote_filters' in valid_structure['options']:
                valid_filters = SchemaCheck(
                    valid_structure['options']['remote_filters'],
                    Schema(filters.Filters(current_action, location=loc)),
                    'filters',
                    '{0}, "filters"'.format(loc)
                ).result()
                clean_remote_filters = validate_filters(
                    current_action, valid_filters)
                clean_config[action_id]['options'].update(
                    { 'remote_filters' : clean_remote_filters }
                )
                
    # if we've gotten this far without any Exceptions raised, it's valid!
    return { 'actions' : clean_config }

###########################
## For Singleton Actions ##
###########################

def filter_schema_check(action, filter_dict):
    valid_filters = SchemaCheck(
        filter_dict,
        Schema(filters.Filters(action, location='singleton')),
        'filters',
        '{0} singleton action "filters"'.format(action)
    ).result()
    return validate_filters(action, valid_filters)

def _prune_excluded(option_dict):
    # This is exclusively to remove options not needed with Singletons
    for k in list(option_dict.keys()):
        if k in EXCLUDE_FROM_SINGLETONS:
            del option_dict[k]
    return option_dict

def option_schema_check(action, option_dict):
    clean_options = SchemaCheck(
        prune_nones(option_dict),
        options.get_schema(action),
        'options',
        '{0} singleton action "options"'.format(action)
    ).result()
    return _prune_excluded(clean_options)
