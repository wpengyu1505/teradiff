spark-submit --class io.rainbow6.teradiff.runner.TeradiffRunner \
    --conf spark.yarn.executor.memoryOverhead=8192 \
    --num-executors 3000 \
    --executor-memory 3G \
    --driver-memory 3G \
    --master yarn-client \
    teradiff-1.0.0.jar \
    source1 \
    source2 \
    table \
    path/to/teradiff.properties \
    3000
