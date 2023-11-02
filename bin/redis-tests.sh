#!/bin/bash
set -e

# change path into root directory
cd $(dirname $(dirname $(readlink -f $0)))

# load params
storagetype=${1}
db=${2}

# Set up connection or other environment variables specific to the Redis test
if [[ $db = "redis" ]]; then
    export REDIS_CONNECTION_STRING="localhost:6379"
else
    echo "unsupported database: $db" >&2
    exit 1
fi

echo "Running tests with $storagetype storage using $db database:"
echo

# Since you mentioned your Redis tests are in the "temporal" folder
listPackages() {
    go list ./temporal/...
}

listPackages | xargs -n1 echo "-"
echo

for pkg in $(listPackages);
do
    coveragefile=`echo "$pkg-$db" | awk -F/ '{print $NF}'`

    tags=""
    if [[ ${pkg} == *"driver"* ]]; then
            tags="-tags $db"
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
