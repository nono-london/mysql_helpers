#!/usr/bin/env bash
set -euo pipefail
umask 077

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
project_directory=$(cd -- "$script_directory/../../.." && pwd -P)
workflow=ci-python
[[ $(basename -- "$script_directory") = "$workflow" ]]
[[ $# = 1 && $1 =~ ^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{16}$ ]] || {
    echo 'Usage: bash local-cleanup.sh <run-id>' >&2; exit 2;
}
run_directory="$project_directory/.github/ci-local/$workflow/$1"
[[ $(realpath -- "$run_directory") = "$run_directory" ]]
record="$run_directory/resources.txt"
[[ ! -L $record ]]
if [[ ! -f $record ]]; then
    echo 'No resource record remains; nothing to remove.'
    exit 0
fi
project_name=$(basename -- "$project_directory" | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9' '-')
project_name=${project_name%-}
resource_name="ci-${project_name:0:30}-$workflow-${1,,}"
declare -A resources=()
while IFS='=' read -r key value; do
    case "$key" in daemon_id|resource_name|container_id|image_id) ;; *) echo 'Invalid record key.' >&2; exit 1 ;; esac
    [[ ! -v resources[$key] ]] || { echo 'Duplicate record key.' >&2; exit 1; }
    resources[$key]=$value
done < "$record"
[[ ${#resources[@]} = 4 && ${resources[resource_name]} = "$resource_name" ]]
[[ ${resources[container_id]} = '' || ${resources[container_id]} =~ ^[a-f0-9]{64}$ ]]
[[ ${resources[image_id]} = '' || ${resources[image_id]} =~ ^sha256:[a-f0-9]{64}$ ]]
[[ $(docker info --format '{{.ID}}') = "${resources[daemon_id]}" ]]

# Confirm ownership before removing any resource; never source the record as code.
container_id=$(docker container ls --all --quiet --no-trunc --filter "name=^/${resource_name}$")
if [[ -n $container_id ]]; then
    [[ ${resources[container_id]} = '' || $container_id = "${resources[container_id]}" ]]
    [[ $(docker inspect --format '{{index .Config.Labels "ci.local.run"}}' "$container_id") = "$resource_name" ]]
    if [[ -n ${resources[image_id]} ]]; then
        [[ $(docker inspect --format '{{.Image}}' "$container_id") = "${resources[image_id]}" ]]
    fi
fi
volume_name=$(docker volume ls --quiet --filter "name=^${resource_name}$")
if [[ -n $volume_name ]]; then
    [[ $volume_name = "$resource_name" ]]
    [[ $(docker volume inspect --format '{{index .Labels "ci.local.run"}}' "$volume_name") = "$resource_name" ]]
fi

result=0
if [[ -n $container_id ]]; then
    if [[ $(docker inspect --format '{{.State.Running}}' "$container_id") = true ]]; then
        if ! docker stop --time 120 "$container_id"; then result=1; fi
    fi
    if ! docker logs "$container_id" > "$run_directory/reports/mysql.log" 2>&1; then result=1; fi
    if ! docker container rm "$container_id"; then result=1; fi
fi
if [[ -n $volume_name ]]; then
    if ! docker volume rm "$volume_name"; then result=1; fi
fi
for directory in venv tmp; do
    [[ ! -L "$run_directory/$directory" ]]
    if [[ -d "$run_directory/$directory" ]]; then
        if ! rm -rf -- "$run_directory/$directory"; then result=1; fi
    fi
done
if [[ $result = 0 ]]; then
    rm -- "$record"
    echo 'Removed this run’s container, database volume, Python environment and temporary files.'
    echo 'Reports retained. The shared MySQL base image is retained; no custom image or build cache was created.'
else
    echo 'Cleanup reported a failure; resource record retained for recovery.' >&2
fi
exit "$result"
