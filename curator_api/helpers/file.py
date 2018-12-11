import os
import re
# import yaml
from curator_api.exceptions import ConfigurationError, FailedExecution

def read_file(myfile):
    """
    Read a file and return the resulting data.

    :arg myfile: A file to read.
    :rtype: str
    """
    try:
        with open(myfile, 'r') as f:
            data = f.read()
        return data
    except IOError as e:
        raise FailedExecution(
            'Unable to read file {0}. Exception: {1}'.format(myfile, e)
        )

# def get_yaml(path):
#     """
#     Read the file identified by `path` and import its YAML contents.

#     :arg path: The path to a YAML configuration file.
#     :rtype: dict
#     """
#     # Set the stage here to parse single scalar value environment vars from
#     # the YAML file being read
#     single = re.compile( r'^\$\{(.*)\}$' )
#     yaml.add_implicit_resolver ( "!single", single )
#     def single_constructor(loader,node):
#         value = loader.construct_scalar(node)
#         proto = single.match(value).group(1)
#         default = None
#         if len(proto.split(':')) > 1:
#             envvar, default = proto.split(':')
#         else:
#             envvar = proto
#         return os.environ[envvar] if envvar in os.environ else default
#     yaml.add_constructor('!single', single_constructor)

#     raw = read_file(path)
#     try:
#         cfg = yaml.load(raw)
#     except yaml.scanner.ScannerError as e:
#         raise ConfigurationError(
#             'Unable to parse YAML file. Error: {0}'.format(e))
#     return cfg