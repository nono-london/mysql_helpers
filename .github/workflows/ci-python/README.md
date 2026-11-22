# CI Python

The workflow in `mysql_helpers/.github/workflows/ci-python.yml` runs the existing
pytest suite on pushes to `master` and pull requests targeting `master`.

Python runs directly on the GitHub-hosted Ubuntu 24.04 runner. Python 3.9 is the
minimum supported version declared by `mysql_helpers/setup.py`. The official
MySQL 8.4.13 image runs as a separate service container and is pinned by digest.
The service image uses Oracle Linux 9 internally. GitHub manages its lifecycle;
there is no custom Docker image, database bootstrap or cleanup script.

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

Use Python 3.9 and Docker. Start the same pinned MySQL image with the four
`MYSQL_*` service settings from `mysql_helpers/.github/workflows/ci-python.yml`,
publishing its port 3306 to a free loopback host port. Wait for the authenticated
health check to pass. In an isolated Python environment, install the requirements
and local package as above. Export all five `MYSQL_DB_*` keys, with the chosen
host port, then run the pytest command. Do not load a development environment
file. Remove the disposable container and its database volume afterwards.

Local test results do not prove GitHub event handling or artifact uploads. View
the repository's Actions tab for the hosted job and downloadable reports.

## References

- [GitHub service containers](https://docs.github.com/en/actions/tutorials/use-containerized-services/use-docker-service-containers)
- [Official MySQL image](https://hub.docker.com/_/mysql)
- [Official image source](https://github.com/docker-library/mysql)
- [Python setup action](https://github.com/actions/setup-python)
