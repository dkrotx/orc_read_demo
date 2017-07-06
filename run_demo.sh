#!/usr/bin/env bash

set -e
set -o pipefail

collect_dep_jars() {
    local res=$( find  ./build/install/orc_read_demo/lib/ -name '*.jar' | xargs -I{} -n1 echo -n '{},' )
    echo "${res: : -1}"
}

DEP_JARS="$( collect_dep_jars )"

rm -rf $2
spark-submit --jars $DEP_JARS \
             --class DataFrameDemo --master local[4] \
             ./build/libs/orc_read_demo.jar $1 $2

# --conf park.executor.userClassPathFirst=true \

