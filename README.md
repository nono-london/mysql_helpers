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