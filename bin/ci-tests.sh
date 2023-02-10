#!/bin/bash
set -e

# change path into root directory
cd $(dirname $(dirname $(readlink -f $0)))

# import common functions
. ./bin/_common.sh

# load params
dbtype=${1}
db=${2}

if [[ $db = "mongo" ]]; then
    export TEST_STORAGE_CONNECTION_STRING="mongodb://localhost:27017/test"
else
    echo "unsupported database: $db" >&2
    exit 1
fi

echo "Running $dbtype  with $db database, testing:"
echo
listPackages $dbtype | xargs -n1 echo "-"
echo

for pkg in $(listPackages);
do
    coveragefile=`echo "$pkg-$db" | awk -F/ '{print $NF}'`

    tags=""
    if [[ ${pkg} == *"driver"* ]]; then
            tags="-tags $db"
    fi

    set -x

     echo "Testing... $pkg with tags $tags coverprofile $coveragefile"
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