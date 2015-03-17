!#/bin/bash

spark-submit --class com.cloudera.ds.svdbench.GenerateMatrix \
  --master yarn-client --executor-memory 20G --num-executors 50 \
  --driver-class-path ./target/svd-benchmark-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  ./target/svd-benchmark-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  --path hdfs://juliet/matrix-svd --master yarn-cluster --nRows 1000 \
  --nCols 100 --nNonZero 30 --blockSize 100

