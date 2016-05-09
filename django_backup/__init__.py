try:
    from django.utils.version import get_version
except ImportError:
    from .get_version import get_version

VERSION = (2, 0, 1, 'beta', 0)

__version__ = get_version(VERSION)
