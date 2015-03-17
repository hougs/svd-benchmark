!#/bin/bash

export SPARK_HOME=/home/juliet/bin/spark-1.3.0-bin-hadoop2.4/bin

$SPARK_HOME/spark-submit --class com.cloudera.ds.svdbench
.GenerateMatrix \
  --conf spark.yarn.jar=hdfs:///user/juliet/bin/spark-1.3.0-bin-hadoop2.4/lib/spark-assembly-1.3
  .0-hadoop2.4.0.jar \
  --master yarn-client --executor-memory 8G --num-executors 50 \
  --driver-class-path ./target/svd-benchmark-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  ./target/svd-benchmark-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  --path hdfs://juliet/matrix-svd --master yarn-cluster --nRows 1000 \
  --nCols 100 --nNonZero 30 --blockSize 100

