#!/bin/bash

spark-submit --class io.rainbow6.teradiff.runner.TeradiffRunner \
    --conf spark.yarn.executor.memoryOverhead=8192 \
    --num-executors 3000 \
    --executor-memory 3G \
    --driver-memory 3G \
    --master yarn-client \
    teradiff-1.0.0.jar \
    $@

# Example command call:
# ./run.sh \
# --left src/test/resources/data1_header.txt \
# --right src/test/resources/data2_header.txt \
# --sourceType csv \
# --propertyFile src/test/resources/test.properties \
# --outputFile target/testSummary.txt \
# --runMode local \
# --partitions 100 \
# --leftDelimiter "|" \
# --rightDelimiter "|" \
# --leftKey "id" \
# --leftValue "id,col1,col2,col3" \
# --rightKey "id" \
# --rightValue "id,col1,col2,col3" \
# --rightWithHeader \
# --leftWithHeader
