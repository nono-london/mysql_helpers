#!/usr/bin/env bash
set -euo pipefail
umask 077

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
project_directory=$(cd -- "$script_directory/../../.." && pwd -P)
workflow=ci-python
mysql_image=mysql:8.4.13@sha256:a81d043930e51745522bd5c430961cf8286f0a23f0a9a2a6e0108f7ea71b0902
[[ $(basename -- "$script_directory") = "$workflow" ]]
cd -- "$project_directory"
[[ $# = 0 ]] || { echo 'Usage: bash local-build.sh' >&2; exit 2; }

# CI_PYTHON selects a local Python 3.9 interpreter; no system environment is changed.
CI_PYTHON=${CI_PYTHON:-python3.9}
command -v "$CI_PYTHON" >/dev/null
"$CI_PYTHON" -c 'import sys; assert sys.version_info[:2] == (3, 9), "Python 3.9 is required"'
source /etc/os-release
[[ $ID = ubuntu && $VERSION_ID = 24.04 ]] || {
    echo 'Local CI requires Ubuntu 24.04, matching the GitHub runner.' >&2; exit 1;
}
daemon_id=$(docker info --format '{{.ID}}')
[[ -n $daemon_id ]]

[[ -z $(git ls-files -- .github/ci-local) ]] || {
    echo 'Refusing a local-state directory containing tracked files.' >&2; exit 1;
}
[[ ! -L .gitignore ]]
if ! git check-ignore --no-index --quiet .github/ci-local/probe; then
    printf '\n/.github/ci-local/\n' >> .gitignore
fi
git check-ignore --no-index --quiet .github/ci-local/probe

state_root="$project_directory/.github/ci-local/$workflow"
mkdir -p -- "$state_root"
[[ $(realpath -- "$state_root") = "$state_root" ]]
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
run_directory="$state_root/$run_id"
mkdir -- "$run_directory"
mkdir -- "$run_directory/reports" "$run_directory/tmp"
project_name=$(basename -- "$project_directory" | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9' '-')
project_name=${project_name%-}
resource_name="ci-${project_name:0:30}-$workflow-${run_id,,}"
container_id=
image_id=
record_resources() {
    printf 'daemon_id=%s\nresource_name=%s\ncontainer_id=%s\nimage_id=%s\n' \
        "$daemon_id" "$resource_name" "$container_id" "$image_id" > "$run_directory/resources.tmp"
    mv -- "$run_directory/resources.tmp" "$run_directory/resources.txt"
}
record_resources
finish() {
    local result=$? cleanup_result report_result
    trap - EXIT INT TERM
    set +e
    bash "$script_directory/local-cleanup.sh" "$run_id" 2>&1 | tee "$run_directory/reports/cleanup.log"
    cleanup_result=$?
    printf '%s\n' "$result" > "$run_directory/reports/build-test-exit-code.txt"
    report_result=$?
    if [[ $result = 0 && $report_result != 0 ]]; then result=$report_result; fi
    printf '%s\n' "$cleanup_result" > "$run_directory/reports/cleanup-exit-code.txt"
    report_result=$?
    if [[ $result = 0 && $report_result != 0 ]]; then result=$report_result; fi
    printf 'Local reports: %s\n' "$run_directory/reports"
    if [[ $result = 0 && $cleanup_result != 0 ]]; then result=$cleanup_result; fi
    exit "$result"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf 'Local run: %s\n' "$run_id"

# Keep dependency build files inside this run and avoid shared pip/bytecode caches.
export TMPDIR="$run_directory/tmp" PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1
export PYTHON_DOTENV_DISABLED=1
"$CI_PYTHON" -m venv "$run_directory/venv" 2>&1 | tee "$run_directory/reports/environment.log"
python_command="$run_directory/venv/bin/python"
{
    "$python_command" -m pip install -r requirements.txt
    "$python_command" -m pip install --no-deps .
} 2>&1 | tee "$run_directory/reports/install.log"

docker pull "$mysql_image" 2>&1 | tee "$run_directory/reports/image-pull.log"
image_id=$(docker image inspect --format '{{.Id}}' "$mysql_image")
record_resources
docker volume create --label "ci.local.run=$resource_name" "$resource_name"
container_id=$(docker create --name "$resource_name" --label "ci.local.run=$resource_name" \
    --mount "type=volume,source=$resource_name,target=/var/lib/mysql" \
    --env MYSQL_ROOT_PASSWORD=ci_root_password --env MYSQL_DATABASE=ci_test \
    --env MYSQL_USER=ci_user --env MYSQL_PASSWORD=ci_password \
    --publish 127.0.0.1::3306 \
    --health-cmd "MYSQL_PWD=ci_password mysql --protocol=TCP --host=127.0.0.1 --user=ci_user --database=ci_test --execute='SELECT 1'" \
    --health-interval 10s --health-timeout 5s --health-start-period 5m --health-retries 12 \
    "$image_id")
record_resources
docker start "$container_id"

ready=false
for ((attempt=0; attempt<225; attempt++)); do
    status=$(docker inspect --format '{{.State.Health.Status}}' "$container_id")
    if [[ $status = healthy ]]; then ready=true; break; fi
    if [[ $status = unhealthy ]]; then break; fi
    sleep 2
done
[[ $ready = true ]] || { echo 'MySQL did not become ready within 450 seconds.' >&2; exit 1; }

export MYSQL_DB_HOST=127.0.0.1 MYSQL_DB_USER=ci_user MYSQL_DB_PASS=ci_password MYSQL_DB_NAME=ci_test
MYSQL_DB_PORT=$(docker inspect --format '{{(index (index .NetworkSettings.Ports "3306/tcp") 0).HostPort}}' "$container_id")
export MYSQL_DB_PORT
{
    cat /etc/os-release
    "$python_command" --version
    docker exec --env MYSQL_PWD=ci_password "$container_id" \
        mysql --protocol=TCP --host=127.0.0.1 --user=ci_user --database=ci_test \
        --execute='SELECT VERSION();'
    printf 'Shared base image: %s\nNo custom Docker image or BuildKit cache is created.\n' "$image_id"
} 2>&1 | tee "$run_directory/reports/versions.log"
"$python_command" -m pytest tests --junitxml="$run_directory/reports/junit.xml" \
    -o "cache_dir=$run_directory/tmp/pytest-cache" 2>&1 | tee "$run_directory/reports/tests.log"
