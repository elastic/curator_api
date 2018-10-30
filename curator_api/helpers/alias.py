from elasticsearch.exceptions import NotFoundError
import logging

logger = logging.getLogger(__name__)

def rollable_alias(client, alias):
    """
    Ensure that `alias` is an alias, and points to an index that can use the
    _rollover API.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg alias: An Elasticsearch alias
    """
    try:
        response = client.indices.get_alias(name=alias)
    except NotFoundError:
        logger.error('alias "{0}" not found.'.format(alias))
        return False
    # Response should be like:
    # {'there_should_be_only_one': {u'aliases': {'value of "alias" here': {}}}}
    # Where 'there_should_be_only_one' is a single index name that ends in a
    # number, and 'value of "alias" here' reflects the value of the passed
    # parameter.
    if len(response) > 1:
        logger.error(
            '"alias" must only reference one index: {0}'.format(response))
    # elif len(response) < 1:
    #     logger.error(
    #         '"alias" must reference at least one index: {0}'.format(response))
    else:
        index = list(response.keys())[0]
        rollable = False
        # In order for `rollable` to be True, the last 2 digits of the index
        # must be digits, or a hyphen followed by a digit.
        # NOTE: This is not a guarantee that the rest of the index name is
        # necessarily correctly formatted.
        if index[-2:][1].isdigit():
            if index[-2:][0].isdigit():
                rollable = True
            elif index[-2:][0] == '-':
                rollable = True
        return rollable