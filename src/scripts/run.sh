#!/bin/bash

SOURCE_LEFT=tablename1
SOURCE_RIGHT=tablename2
SOURCE_TYPE=table
#SOURCE_TYPE=csv
PROPERTY_FILE=./teradiff.properties
NUM_PARTITIONS=3000

spark-submit --class io.rainbow6.teradiff.runner.TeradiffRunner \
    --conf spark.yarn.executor.memoryOverhead=8192 \
    --num-executors 3000 \
    --executor-memory 3G \
    --driver-memory 3G \
    --master yarn-client \
    teradiff-1.0.0.jar \
    $SOURCE_LEFT \
    $SOURCE_LEFT \
    $SOURCE_TYPE \
    $PROPERTY_FILE \
    $NUM_PARTITIONS
