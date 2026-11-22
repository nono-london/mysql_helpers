# CI Python

The workflow in `mysql_helpers/.github/workflows/ci-python.yml` runs the existing
pytest suite on pushes to `master` and pull requests targeting `master`.

Python runs directly on the GitHub-hosted Ubuntu 24.04 runner. Python 3.9 is the
minimum supported version declared by `mysql_helpers/setup.py`. The official
MySQL 8.4.13 image runs as a separate service container and is pinned by digest.
The service image uses Oracle Linux 9 internally. GitHub manages its lifecycle.
The local verification scripts manage their own resources separately.

## Installation and tests

The workflow installs dependencies from `mysql_helpers/requirements.txt`, then
installs the local package with `python -m pip install --no-deps .`. It runs:

```bash
python -m pytest tests --junitxml="$CI_REPORT_DIRECTORY/junit.xml"
```

The official image creates a temporary `ci_test` database and `ci_user` account.
An authenticated TCP health check confirms readiness. Its five-minute startup
grace period allows database initialisation on slower machines; a successful
check allows tests to proceed immediately. GitHub assigns a free host
port, supplied through `MYSQL_DB_PORT`. The other four database keys documented
in `mysql_helpers/.env.example` receive explicit temporary CI values. Tests create
their own temporary tables; no migrations or external fixtures are needed.
Private environment files are not used by the workflow. No repository files are
copied into the MySQL image. Temporary CI passwords grant access only to the
service created for that job; no GitHub Secrets are required.

Pytest output and JUnit XML, plus available MySQL logs, are uploaded even after
failure and retained for seven days. Bash pipeline failure propagation preserves
a failed pytest exit status through `tee`. No tests are skipped by the workflow.
The job has a 15-minute timeout covering service startup, installation and tests.

## Local verification

Use Ubuntu 24.04, Bash, Git, Python 3.9 with venv support and an accessible Docker
daemon. From the checkout root, run:

```bash
bash .github/workflows/ci-python/local-build.sh
```

If Python 3.9 is not on the command path, set `CI_PYTHON` to its executable.
The script checks the Ubuntu and Python versions, creates an isolated Python
environment and uses the same pinned official MySQL image as GitHub. It installs
the declared dependencies, waits for authenticated readiness and runs the full
suite. Private environment-file loading is disabled. No development database
is contacted.

Run records and reports go under
`mysql_helpers/.github/ci-local/ci-python/<run-id>/`. Before creating that
directory, the script ensures `mysql_helpers/.gitignore` excludes
`/.github/ci-local/` and refuses tracked local state.

On success, failure, SIGINT or SIGTERM, cleanup stops and removes the run's
container, removes its labelled database volume and deletes its isolated Python
environment and temporary dependency/test files. Pip caching and Python bytecode
generation are disabled. This service-container design builds no custom Docker
image and creates no BuildKit cache; the shared official MySQL base image remains.
No global Docker prune or shared-cache deletion is performed.

Each run records the Docker daemon ID, planned resource name, container ID and
base-image ID in `resources.txt`. Cleanup checks the daemon, resource IDs and
ownership labels. Test failures retain their exit status; cleanup or logging
failure also fails otherwise successful verification. Reports remain after
cleanup. The resource record is deleted only after successful cleanup.

A forced kill can prevent automatic cleanup. From the same checkout and Docker
daemon, replace `<run-id>` with the interrupted run's identifier and run:

```bash
bash .github/workflows/ci-python/local-cleanup.sh <run-id>
```

The command is safe to repeat. A cleanup failure retains the resource record
for recovery; successful cleanup retains only the reports. The cloud workflow
does not invoke either local script.

Local test results do not prove GitHub event handling or artifact uploads. View
the repository's Actions tab for the hosted job and downloadable reports.

## References

- [GitHub service containers](https://docs.github.com/en/actions/tutorials/use-containerized-services/use-docker-service-containers)
- [Official MySQL image](https://hub.docker.com/_/mysql)
- [Official image source](https://github.com/docker-library/mysql)
- [Python setup action](https://github.com/actions/setup-python)
