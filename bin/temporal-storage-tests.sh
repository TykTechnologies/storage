#!/bin/bash
set -e

# change path into root directory
cd $(dirname $(dirname $(readlink -f $0)))

# load params
database=${1}
version=${2}

if [[ $database = "redis" ]]; then
    export REDIS_CONNECTION_STRING="localhost:6379"
else
    echo "unsupported database: $database" >&2
    exit 1
fi

echo "Running tests with using $database version: $version :"
echo


listPackages() {
    go list ./temporal/...
}

listPackages | xargs -n1 echo "-"
echo

for pkg in $(listPackages);
do
    coveragefile=`echo "$pkg-$database" | awk -F/ '{print $NF}'`

    tags=""
    if [[ ${pkg} == *"driver"* ]]; then
            tags="-tags $database"
    fi

    set -x

    echo "Testing... $pkg with tags $tags"
    go test \
    -failfast \
    -timeout ${TEST_TIMEOUT:-"5m"} \
    -race \
    -cover \
    $tags \
    -coverprofile=${coveragefile}.cov \
    -v ${pkg}
    set +x
done
