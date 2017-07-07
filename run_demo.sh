#!/usr/bin/env bash

set -e
set -o pipefail

collect_dep_jars() {
    local res=$( find  ./build/install/orc_read_demo/lib/ -name '*.jar' | xargs -I{} -n1 echo -n '{},' )
    echo "${res: : -1}"
    #echo ./build/install/orc_read_demo/lib/hive-storage-api-2.2.1.jar,./build/install/orc_read_demo/lib/orc-core-1.4.0.jar,./build/install/orc_read_demo/lib/orc-mapreduce-1.4.0.jar
    #echo ./build/install/orc_read_demo/lib/orc-core-1.4.0.jar,./build/install/orc_read_demo/lib/orc-mapreduce-1.4.0.jar
}

DEP_JARS="$( collect_dep_jars )"

exec spark-submit --jars $DEP_JARS --class DataFrameDemo --master local[4] \
     ./build/libs/orc_read_demo.jar $1
