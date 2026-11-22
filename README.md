# mysql_helpers

[![CI Python](https://github.com/nono-london/mysql_helpers/actions/workflows/ci-python.yml/badge.svg)](https://github.com/nono-london/mysql_helpers/actions/workflows/ci-python.yml)

A Python wrapper around MySQL Connector/Python for synchronous queries,
asynchronous queries and synchronous connection pooling. Fetch results as Python
rows or pandas DataFrames, execute parameterised statements and attach application
identity to database connections.

## Contents

- [Installation](#installation)
- [Configuration](#configuration)
- [Choose a connector](#choose-a-connector)
- [Synchronous queries](#synchronous-queries)
- [Asynchronous queries](#asynchronous-queries)
- [Connection pooling](#connection-pooling)
- [DataFrames](#dataframes)
- [Inserts, updates and deletes](#inserts-updates-and-deletes)
- [API behaviour and limitations](#api-behaviour-and-limitations)
- [Application identity](#application-identity)
- [Tests and CI](#tests-and-ci)

## Installation

The package declares Python **3.9 or later** in
[mysql_helpers/setup.py](setup.py). A running MySQL server and access to an
existing database are required.

Install from GitHub:

```bash
python -m pip install git+https://github.com/nono-london/mysql_helpers.git
```

Dependencies include `mysql-connector-python`, `python-dotenv` and `pandas`.

## Configuration

Each connector accepts these optional constructor arguments. When an argument is
`None`, the connector reads the corresponding environment variable:

| Constructor argument | Environment variable | Purpose |
| --- | --- | --- |
| `db_host` | `MYSQL_DB_HOST` | Server hostname or IP address |
| `db_port` | `MYSQL_DB_PORT` | Server port, commonly `3306` |
| `db_user` | `MYSQL_DB_USER` | Database username |
| `db_password` | `MYSQL_DB_PASS` | Database password |
| `db_name` | `MYSQL_DB_NAME` | Existing database name |

All five settings must be supplied through arguments or the environment. Missing
environment variables raise `KeyError`; the wrapper does not supply defaults.

For local configuration, copy [mysql_helpers/.env.example](.env.example) to
`mysql_helpers/.env` in your checkout and fill in your connection settings.
Keep that file private. The connector classes do **not** load environment files
automatically: call `load_dotenv()` before constructing a connector, as shown
below, or export the variables through your application's environment.

## Choose a connector

| Connector | Use | Module |
| --- | --- | --- |
| `MySQLConnectorNative` | One synchronous connection | `mysql_helpers.mysql_con.mysql_sync` |
| `MySQLConnectorNativeAsync` | One asynchronous connection | `mysql_helpers.mysql_con.mysql_async` |
| `MySQLConnectorPoolNative` | A synchronous pool with optional named connection reuse | `mysql_helpers.mysql_con.mysql_pool_sync` |

The single-connection helpers open a connection on the first query and reuse an
open connection. The pooled helper creates its pool during construction.
Call `open_connection()` to open a single connection explicitly, awaiting the
method for the asynchronous helper.
There is no asynchronous pool or context-manager interface.

## Synchronous queries

```python
from dotenv import load_dotenv

from mysql_helpers.mysql_con.mysql_sync import MySQLConnectorNative

load_dotenv()
db = MySQLConnectorNative()
rows = db.fetch_all_as_dicts("SELECT %s AS answer", sql_variables=(42,))
if rows is None:
    raise RuntimeError("The query failed; check the application log.")
print(rows)  # [{'answer': 42}]
```

Use `%s` placeholders for values and pass `sql_variables` as a tuple. A single
value requires a trailing comma, as in `(42,)`. Do not insert values into SQL
with string formatting. Placeholders represent values, not table or column names.

Query methods default to `close_connection=True`. To retain a single connection
between queries, pass `close_connection=False` and call `db.close_connection()`
when finished, preferably in a `finally` block.

## Asynchronous queries

```python
import asyncio

from dotenv import load_dotenv

from mysql_helpers.mysql_con.mysql_async import MySQLConnectorNativeAsync


async def read_answer():
    load_dotenv()
    db = MySQLConnectorNativeAsync()
    try:
        rows = await db.fetch_all_as_dicts(
            "SELECT %s AS answer", sql_variables=(42,), close_connection=False
        )
        if rows is None:
            raise RuntimeError("The query failed; check the application log.")
        print(rows)  # [{'answer': 42}]
    finally:
        await db.close_connection()


asyncio.run(read_answer())
```

Await the asynchronous helper's query, open and close methods. In an application
that already has an event loop, await `read_answer()` within that loop instead of
calling `asyncio.run()`.

## Connection pooling

```python
from dotenv import load_dotenv

from mysql_helpers.mysql_con.mysql_pool_sync import MySQLConnectorPoolNative

load_dotenv()
db = MySQLConnectorPoolNative(pool_name="readme_pool", pool_size=2)
try:
    for value in (42, 43):
        rows = db.fetch_all_as_dicts(
            "SELECT %s AS answer",
            sql_variables=(value,),
            close_connection=False,
            connection_name="report",
        )
        if rows is None:
            raise RuntimeError("The query failed; check the application log.")
        print(rows)  # [(42,)], then [(43,)]
finally:
    db.close_all_connections()
```

`pool_size` defaults to `30`; values above `32` are capped at `32`. `pool_name`
identifies the pool, while `connection_name` identifies a connection retained by
this helper. Use `close_connection=False` and the same non-empty name on each
call that must reuse that connection, including calls using temporary tables.

By default, successful pooled queries return their connection to the pool.
`db.close_connection("report")` returns one retained connection;
`db.close_all_connections()` returns all retained connections. These methods do
not shut down the pool. Closed entries remain in the helper's name mapping, so
use named connections until their final cleanup rather than reusing a closed
entry.

**Current return format:** the pooled `fetch_all_as_dicts()` method returns a
list of **tuples**, despite its name. The single synchronous and asynchronous
versions return dictionaries. The example above reflects the current behaviour.

## DataFrames

All three connectors provide `fetch_all_as_df()`. Await the method when using
the asynchronous connector.

```python
import pandas as pd
from dotenv import load_dotenv

from mysql_helpers.mysql_con.mysql_sync import MySQLConnectorNative

load_dotenv()
db = MySQLConnectorNative()
result_df = db.fetch_all_as_df("SELECT %s AS answer", sql_variables=(42,))
if not isinstance(result_df, pd.DataFrame):
    raise RuntimeError("The query failed; check the application log.")
print(result_df.to_dict(orient="records"))  # [{'answer': 42}]
```

## Inserts, updates and deletes

Use `execute_one_query()` for one statement with optional parameters. It returns
the affected-row count when execution succeeds. The following example creates a
temporary table, inserts a row and reads it back on the same connection:

```python
from dotenv import load_dotenv

from mysql_helpers.mysql_con.mysql_sync import MySQLConnectorNative

load_dotenv()
db = MySQLConnectorNative()
try:
    db.execute_one_query(
        "CREATE TEMPORARY TABLE readme_values (value INT)",
        close_connection=False,
    )
    affected = db.execute_one_query(
        "INSERT INTO readme_values (value) VALUES (%s)",
        sql_variables=(42,),
        close_connection=False,
    )
    rows = db.fetch_all_as_dicts(
        "SELECT value FROM readme_values", close_connection=False
    )
    if affected != 1 or rows != [{"value": 42}]:
        raise RuntimeError("The write could not be verified; check the log.")
    print(rows)  # [{'value': 42}]
finally:
    db.close_connection()
```

Every successful `execute_one_query()` call explicitly commits. The single
synchronous connector additionally accepts `autocommit=True` by default.
Setting it to `False` does not stop `execute_one_query()` from committing.
The asynchronous and pooled constructors do not expose this option.

## API behaviour and limitations

Each connector supplies `fetch_all_as_dicts()`, `fetch_all_as_df()` and
`execute_one_query()`. Each method accepts `sql_query`, optional `sql_variables`
and `close_connection=True`. Only the pooled methods accept `connection_name`.

| Operation | Single synchronous | Single asynchronous | Pooled synchronous |
| --- | --- | --- | --- |
| `fetch_all_as_dicts()` on success | List of dictionaries | List of dictionaries | List of tuples |
| `fetch_all_as_df()` on success | pandas DataFrame | pandas DataFrame | pandas DataFrame |
| `execute_one_query()` on success | Affected-row count | Affected-row count | Affected-row count |
| Explicit connection cleanup | `close_connection()` | `await close_connection()` | `close_connection(name)` or `close_all_connections()` |

Connection failures raise `ConnectionError`: when the single helpers first open
a connection, or during the pooled helper's construction. Missing configuration
can fail earlier. All constructors accept `raise_on_warnings=False`; setting it
to `True` asks the underlying connector to treat server warnings as errors.

Query errors are logged and often caught rather than raised. Fetch methods
usually return `None` on failure, but the asynchronous DataFrame method currently
can return an object other than a DataFrame after a query error. Check its result with
`isinstance(result, pandas.DataFrame)`. Some error paths can raise exceptions.

There is no unique return value signalling a failed write: the returned count may be
`0`, or an earlier row count if a later step failed. Do not interpret `0` as
proof of failure or a positive count as proof of a committed write. Inspect logs
and verify important writes. The wrapper provides no transaction context,
rollback helper or bulk-execution method.

## Application identity

All three constructors accept `app_name`, `app_function` and a `conn_attrs`
dictionary. These describe the application when a physical connection is opened:

```python
from dotenv import load_dotenv

from mysql_helpers.mysql_con.mysql_sync import MySQLConnectorNative

load_dotenv()
db = MySQLConnectorNative(
    app_name="billing-service",
    app_function="invoice-processing",
    conn_attrs={"deploy_env": "production", "version": "1.2.0"},
)
```

The helper copies `conn_attrs`. Explicit non-`None` `app_name` and `app_function`
arguments override matching dictionary entries; empty strings are passed through.
An empty dictionary is omitted from the underlying connection arguments.

Attributes belong to a physical connection, not an individual query. Changing
`db.conn_attrs` does not update an existing session. Close and reopen a single
connection to send new values, or create a new pooled helper for a new identity.

MySQL Connector/Python validates custom attribute names and values. Use strings,
names of at most 32 characters and values of at most 1024 characters; avoid names
starting with `_`, reserved for connector metadata. Invalid attributes cause a
`ConnectionError` when the connection or pool is established.

With Performance Schema enabled and a nonzero
`performance_schema_session_connect_attrs_size`, an account with permission to
read the following table can query attributes on the same connection that
supplied them:

```sql
SELECT * FROM performance_schema.session_connect_attrs
WHERE processlist_id = CONNECTION_ID();
```

These attributes do not appear in `SHOW PROCESSLIST`. An insufficient Performance
Schema buffer can truncate stored attributes; check
`Performance_schema_session_connect_attrs_lost`. See the
[MySQL connection attribute documentation](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-connection-attribute-tables.html).

## Tests and CI

From a source checkout, install the declared development dependencies and the
local package:

```bash
python -m pip install -r requirements.txt
python -m pip install --no-deps .
python -m pytest tests
```

The full suite needs a MySQL database and the five configuration settings above.
Use a dedicated test database whose account can create temporary tables.

[CI Python](.github/workflows/ci-python.yml) runs on pushes to `master` and pull
requests targeting `master`. It uses Python 3.9 on Ubuntu 24.04 with an official
MySQL 8.4 service container, temporary credentials and downloadable test reports.
See [mysql_helpers/.github/workflows/ci-python/README.md](.github/workflows/ci-python/README.md)
for the setup and report details.

## Further reading

- [MySQL Connector/Python connection arguments](https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html)
- [MySQL Connector/Python connection pooling](https://dev.mysql.com/doc/connector-python/en/connector-python-connection-pooling.html)
