!#/bin/bash

export SPARK_HOME=/home/juliet/bin/spark-1.3.0-bin-hadoop2.4/bin
export HADOOP_CONF_DIR=/etc/hadoop/conf

$SPARK_HOME/spark-submit --class com.cloudera.ds.svdbench.GenerateMatrix \
  --conf spark.yarn.jar=hdfs:///user/juliet/bin/spark-1.3.0-bin-hadoop2.4/lib/spark-assembly-1.3.0-hadoop2.4.0.jar \
  --master yarn-client --executor-memory 8G --num-executors 10 \
  --driver-class-path ./target/svd-benchmark-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  ./target/svd-benchmark-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  --path hdfs:///user/juliet/matrix-svd --nRows 1000 \
  --nCols 100 --fracNonZero 0.01 --blockSize 100

