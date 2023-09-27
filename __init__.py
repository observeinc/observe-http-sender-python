"""observe_http_sender
    Observer observation submission class to HTTP endpoint
"""

import sys

#: The release version
version = '1.3.2'
__version__ = version

MIN_PYTHON_VERSION = 3, 7
MIN_PYTHON_VERSION_STR = '.'.join([str(v) for v in MIN_PYTHON_VERSION])

if sys.version_info <= MIN_PYTHON_VERSION:
    raise Exception(f"ObserveHttpSender {version} requires Python {MIN_PYTHON_VERSION_STR} or newer.")