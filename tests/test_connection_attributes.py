"""Connection metadata forwarding without a running MySQL server."""
import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from mysql.connector import MySQLConnection
from mysql.connector.errors import InterfaceError

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


def test_pool_creation_failure_raises_with_cause(monkeypatch):
    """A pool that can't be built must raise, not leave mysql_pool set to None."""
    cause = InterfaceError("Attribute name 'deploy_environment_name_far_too_long' "
                           "exceeds 32 characters limit size")
    monkeypatch.setattr(mysql_pool_sync, 'MySQLConnectionPool', Mock(side_effect=cause))

    with pytest.raises(ConnectionError) as excinfo:
        mysql_pool_sync.MySQLConnectorPoolNative(
            db_host='localhost', db_port=3306, db_user='test',
            db_password='test', db_name='test', app_name='billing')

    assert excinfo.value.__cause__ is cause
    assert str(cause) in str(excinfo.value)


@pytest.mark.parametrize('options', [
    {'app_name': 'billing-service', 'app_function': 'invoice-processing'},
    {'conn_attrs': {'deploy_env': 'production', 'version': '1.2.0'}},
])
def test_forwarded_attributes_accepted_by_connector(connect_helper, options):
    """What the helpers forward must survive the connector's own validation.

    config() performs no I/O, so this needs no running server.
    """
    _, kwargs = connect_helper(**options)
    MySQLConnection().config(conn_attrs=kwargs['conn_attrs'])


@pytest.mark.parametrize('conn_attrs', [
    {'a' * 33: 'value'},
    {'_reserved': 'value'},
    {'oversized': 'v' * 1025},
    {'not_a_string': 1},
])
def test_connector_rejects_invalid_attributes(conn_attrs):
    """Pins the limits the README documents; the connector owns the rules, we only forward."""
    with pytest.raises(InterfaceError):
        MySQLConnection().config(conn_attrs=conn_attrs)


@pytest.mark.parametrize('error_type', ['interface', 'unexpected'])
def test_pool_creation_failure_preserves_cause(monkeypatch, caplog, error_type):
    from mysql.connector.errors import InterfaceError

    error = InterfaceError('Invalid connection attributes') if error_type == 'interface' else RuntimeError('Pool failed')
    monkeypatch.setattr(mysql_pool_sync, 'MySQLConnectionPool', Mock(side_effect=error))
    with pytest.raises(ConnectionError, match='Failed to create the database connection pool') as caught:
        mysql_pool_sync.MySQLConnectorPoolNative(
            db_host='localhost', db_port=3306, db_user='test',
            db_password='test', db_name='test', app_name='billing',
        )
    assert caught.value.__cause__ is error
    assert str(error) in str(caught.value)
    assert "Can't find MySql server" not in caplog.text


@pytest.mark.parametrize('attrs', [
    {'x' * 33: 'value'},
    {'app_name': 'x' * 1025},
    {'_custom': 'value'},
    {42: 'value'},
    {'app_name': 42},
])
def test_pool_rejects_invalid_attributes_before_connecting(monkeypatch, attrs):
    from mysql.connector.errors import InterfaceError

    # Keep real pool configuration validation; forbid opening physical connections.
    add_connection = Mock(side_effect=AssertionError('Unexpected connection attempt'))
    monkeypatch.setattr(mysql_pool_sync.MySQLConnectionPool, 'add_connection', add_connection)
    with pytest.raises(ConnectionError) as caught:
        mysql_pool_sync.MySQLConnectorPoolNative(
            db_host='localhost', db_port=3306, db_user='test',
            db_password='test', db_name='test', conn_attrs=attrs,
        )
    assert isinstance(caught.value.__cause__, InterfaceError)
    add_connection.assert_not_called()


def test_connector_accepts_attribute_length_boundaries():
    from mysql.connector import MySQLConnection

    connection = MySQLConnection()
    connection.config(conn_attrs={'x' * 32: 'y' * 1024, 'app_name': 'a' * 33})
    assert not connection.is_connected()
