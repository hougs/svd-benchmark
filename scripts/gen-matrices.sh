!#/bin/bash

./bin/spark-submit \
  --class com.cloudera.ds.svdbench.GenerateMatrix \
  --master yarn-cluster \  # can also be `yarn-client` for client mode
  --executor-memory 20G \
  --num-executors 50 \
  /target/svd-benchmark-0.0.1-SNAPSHOT.jar \
  hdfs://juliet/matrix-svd yarn-cluster 1000 100 30 100

