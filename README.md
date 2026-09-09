mysql_helpers
==

# configure app

## create .env file
You need to create a .env file at the root of the project.
Use .env.example to get a template of the info that need to be provided to ensure connection to your MySQL DB



# MySQL Docs & Tutorials
## Connection Pooling
### Usage
* the API allow user to store a connection by a given name and reuse it with the set of fetch_all_as_dicts, fetch_all_as_df, execute_one_query
* connection can be closed at the end of the program or batched closed, using close_connection(connection_name:...) or close_all_connection

### Docs
 * [MySQL doc](https://dev.mysql.com/doc/connector-python/en/connector-python-connection-pooling.html)


# Useful Git commands
* remove files git repository (not the file system)
    ```
    git rm --cached <path of file to be removed from git repo.>
    git commit -m "Deleted file from repository only"
    git push
    ```
* cancel command:
    ```
    git restore --staged .
    ```
* create tags/versions on GitHub
    ```
    git tag <version_number>
    git push origin <version_number>

    ```

## Application identity at connection establishment

All three helpers (`MySQLConnectorNative`, `MySQLConnectorNativeAsync`, and
`MySQLConnectorPoolNative`) accept optional `app_name`, `app_function`, and
`conn_attrs` parameters:

```python
from mysql_helpers.mysql_con.mysql_sync import MySQLConnectorNative

client = MySQLConnectorNative(
    app_name="billing-service",
    app_function="invoice-processing",
    conn_attrs={"deploy_env": "production", "version": "1.2.0"},
)
```

Database settings in this example come from the existing environment variables.
The helpers copy `conn_attrs`; explicit non-`None` app parameters override matching
keys. Empty strings are passed through. If the merged dictionary is empty, the
connector argument is omitted, preserving existing connector behavior.

Attributes describe a physical connection and are sent when it is established.
Changing `client.conn_attrs` does not update an already connected session; close
and reopen the sync/async connection to send new values. Pools use the attributes
provided at pool creation for their connections, so use them for app identity,
not per-request metadata. Create a new pool to use a different identity.

Note that `MySQLConnectorPoolNative(...)` now raises `ConnectionError` when the pool
cannot be built, rather than returning an instance whose `mysql_pool` is `None` and
failing later on the first query.

The connector validates attribute names and values (strings, at most 32 and 1024
characters respectively). Avoid names beginning with `_`, reserved for connector
metadata. Invalid attributes make the connector raise `InterfaceError`; all three
helpers surface that as a `ConnectionError` with the original exception chained.
The sync and async helpers raise on the first connection attempt, the pool helper
at construction time.

With Performance Schema enabled and a nonzero
`performance_schema_session_connect_attrs_size`, inspect attributes on the same
connection with:

```sql
SELECT * FROM performance_schema.session_connect_attrs
WHERE processlist_id = CONNECTION_ID();
```

These attributes do not appear in `SHOW PROCESSLIST`. An insufficient Performance
Schema buffer can truncate attributes without a client error; check
`Performance_schema_session_connect_attrs_lost`. The server rejects connections
whose aggregate attribute payload exceeds 64 KB.
