"""
Client modules for AWS Lambda Architecture
"""

from .rds_connection import get_rds_connection_string
from .rds_timescale_client import RDSTimescaleClient


def __getattr__(name):
    if name == 'PolygonClient':
        from .polygon_client import PolygonClient
        globals()['PolygonClient'] = PolygonClient
        return PolygonClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    'PolygonClient',
    'RDSTimescaleClient',
    'get_rds_connection_string',
]
