"""
tests/conftest.py
Inject a minimal mysql.connector stub BEFORE any project module imports it.
This allows the full test suite to run without installing mysql-connector-python.
"""
from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

# ── Build a fake mysql.connector package tree ────────────────────────────────

def _make_mysql_stub() -> None:
    if "mysql" in sys.modules:
        return                          # already present (real install)

    # mysql
    mysql_pkg = types.ModuleType("mysql")
    mysql_pkg.__path__ = []

    # mysql.connector
    connector = types.ModuleType("mysql.connector")
    connector.__path__ = []

    # mysql.connector.pooling
    pooling = types.ModuleType("mysql.connector.pooling")

    class FakePool:
        def __init__(self, **kw):
            pass
        def get_connection(self):
            return MagicMock()

    pooling.MySQLConnectionPool = FakePool

    # mysql.connector.Error
    class FakeMySQLError(Exception):
        pass

    connector.Error   = FakeMySQLError
    connector.pooling = pooling

    # register all three in sys.modules
    sys.modules["mysql"]                    = mysql_pkg
    sys.modules["mysql.connector"]          = connector
    sys.modules["mysql.connector.pooling"]  = pooling

    mysql_pkg.connector = connector


_make_mysql_stub()
