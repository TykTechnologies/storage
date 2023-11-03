#!/bin/bash
set -e

# change path into root directory
cd $(dirname $(dirname $(readlink -f $0)))

# import common functions
. ./bin/_common.sh

# load params
dbtype=${1}
db=${2}

# set the connection string and package list based on the database type
case $db in
    "mongo")
        export TEST_STORAGE_CONNECTION_STRING="mongodb://localhost:27017/test"
        packages=$(listPackages persistent)
        ;;
    "redis")
        export REDIS_CONNECTION_STRING="localhost:6379"
        packages=$(listPackages temporal)
        ;;
    *)
        echo "unsupported database: $db" >&2
        exit 1
        ;;
esac

echo "Running tests using $dbtype with $db database, testing:"
echo
echo $packages | xargs -n1 echo "-"
echo

# function to run tests for a package
run_tests() {
    local pkg=$1
    local coveragefile=`echo "$pkg-$db" | awk -F/ '{print $NF}'`

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
}

# loop through the packages and run tests
for pkg in $packages; do
    run_tests $pkg
done
