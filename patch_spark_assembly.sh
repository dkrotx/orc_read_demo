#!/usr/bin/env bash

set -e
set -o pipefail

err() {
    echo Error: "$@" >&2
    exit 1
}

./gradlew jar
HIVE_STORAGE_API=$( readlink -f ./build/install/orc_read_demo/lib/hive-storage-api-*.jar )
[[ -e $HIVE_STORAGE_API ]] || err "$HIVE_STORAGE_API does not exists even after build"

[[ -n $SPARK_HOME ]] || err "env SPARK_HOME is not set"
SPARK_ASSEMBLY=$( readlink -f $SPARK_HOME/lib/spark-assembly-1.6.3-hadoop2.6.0.jar )
[[ -e $SPARK_ASSEMBLY ]] || err "$SPARK_ASSEMBLY not found - do you use Spark 1.x?"

TJARDIR=$( mktemp -t -d patch_spark_assembly.XXXXXXXX )

## the patch itself
cd $TJARDIR
jar -xf $SPARK_ASSEMBLY
rm -rf org/apache/hive/ org/apache/hadoop/hive/
mkdir __subdir
cd __subdir
jar -xf $HIVE_STORAGE_API
cp -R org ../
cd .. && rm -rf __subdir
jar -cf $( basename $SPARK_ASSEMBLY ) *

echo
echo New spark-assembly is ready: $TJARDIR/$( basename $SPARK_ASSEMBLY )
echo You can replace original one by $ sudo install -b.bak $( dirname $SPARK_ASSEMBLY)/
