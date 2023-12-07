mysql_helpers
==

# configure app

## create .env file
You need to create a .env file at the root of the project.
Use .env.example to get a template of the info that need to be provided to ensure connection to your MySQL DB



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