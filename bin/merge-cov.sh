#!/bin/bash
set -e

# change path into root directory
cd $(dirname $(dirname $(readlink -f $0)))

# import common functions
. ./bin/_common.sh

for pkg in $(listPackages);
do
    coveragefile=`echo "$pkg.cov" | awk -F/ '{print $NF}'`
    mongo_cov=`echo "$pkg-mongo.cov" | awk -F/ '{print $NF}'`
    set -x
    gocovmerge $mongo_cov  > $coveragefile
    set +x
    rm $mongo_cov
done