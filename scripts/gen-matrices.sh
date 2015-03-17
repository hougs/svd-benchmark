!#/bin/bash

spark-submit --class com.cloudera.ds.svdbench.GenerateMatrix \
  --master yarn-cluster --executor-memory 20G --num-executors 50 \
  --driver-class-path target/svd-benchmark-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  target/svd-benchmark-0.0.1-SNAPSHOT-jar-with-dependencies.jar hdfs://juliet/matrix-svd \
  yarn-cluster 1000 100 30 100

