"""Connection metadata forwarding without a running MySQL server."""
import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from mysql_helpers.mysql_con import mysql_async, mysql_pool_sync, mysql_sync


@pytest.fixture(params=['sync', 'async', 'pool'])
def connect_helper(request, monkeypatch):
    mode = request.param
    factory = AsyncMock() if mode == 'async' else Mock()
    if mode == 'async':
        module, target, cls = mysql_async, '_connect', mysql_async.MySQLConnectorNativeAsync
        factory.return_value.is_connected = AsyncMock(return_value=True)
    elif mode == 'pool':
        module, target, cls = mysql_pool_sync, 'MySQLConnectionPool', mysql_pool_sync.MySQLConnectorPoolNative
    else:
        module, target, cls = mysql_sync, 'MySQLConnection', mysql_sync.MySQLConnectorNative
        factory.return_value.is_connected.return_value = True
    monkeypatch.setattr(module, target, factory)

    def connect(**kwargs):
        helper = cls(db_host='localhost', db_port=3306, db_user='test',
                     db_password='test', db_name='test', **kwargs)
        if mode == 'async':
            asyncio.run(helper.open_connection())
            asyncio.run(helper.open_connection())
        elif mode == 'sync':
            helper.open_connection()
            helper.open_connection()
        factory.assert_called_once()
        return helper, factory.call_args.kwargs

    return connect


@pytest.mark.parametrize('options', [{}, {'conn_attrs': {}},
                                    {'app_name': None, 'app_function': None, 'conn_attrs': None}])
def test_empty_attributes_omit_connector_argument(connect_helper, options):
    _, kwargs = connect_helper(**options)
    assert 'conn_attrs' not in kwargs


@pytest.mark.parametrize('options, expected', [
    ({'app_name': 'billing'}, {'app_name': 'billing'}),
    ({'app_function': 'invoices'}, {'app_function': 'invoices'}),
    ({'conn_attrs': {'deploy_env': 'staging', 'app_name': 'fallback'}},
     {'deploy_env': 'staging', 'app_name': 'fallback'}),
    ({'app_name': '', 'app_function': ''}, {'app_name': '', 'app_function': ''}),
])
def test_attributes_forwarded(connect_helper, options, expected):
    _, kwargs = connect_helper(**options)
    assert kwargs['conn_attrs'] == expected


def test_explicit_identity_overrides_without_mutating_input(connect_helper):
    original = {'app_name': 'fallback', 'app_function': 'fallback', 'version': '1.2'}
    helper, kwargs = connect_helper(conn_attrs=original, app_name='billing', app_function='invoices')
    assert kwargs['conn_attrs'] == {'app_name': 'billing', 'app_function': 'invoices', 'version': '1.2'}
    assert original == {'app_name': 'fallback', 'app_function': 'fallback', 'version': '1.2'}
    original['version'] = 'changed'
    assert helper.conn_attrs['version'] == '1.2'
    kwargs['conn_attrs']['version'] = 'connector mutation'
    assert helper.conn_attrs['version'] == '1.2'
