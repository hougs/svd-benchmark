!#/bin/bash
# This script generates a random matrix with the specified number of rows and columns. Every entry
# in this matrix is a zero or a one. The elements are specified by drawing from the uniform
# distribution on (0, 1) for every element in the matrix and setting the value to 1 is the the
# sample is less than the specified sparisty level or to zero otherwise. This allows us to
# approximately specify the percentage of nonzero elements.
#
# usage: gen-matrices.sh outputPath nRows nCols fracNonZero blockSize master
# nRows and nCols specify the number of rows and columns in the generated matrix. fracNonZero is
# the approximate fraction of nonzero elements in the generated matrix. blockSize
OUT_PATH=$1
N_ROWS=$2
N_COLS=$3
FRAC_NON=$4
BLOCK_SIZE=$5
MASTER=$6

export SPARK_HOME=/home/juliet/bin/spark-1.3.0-bin-hadoop2.4/bin
export HADOOP_CONF_DIR=/etc/hadoop/conf

$SPARK_HOME/spark-submit --class com.cloudera.ds.svdbench.GenerateMatrix \
  --verbose --conf spark.yarn.jar=hdfs:///user/juliet/bin/spark-1.3.0-bin-hadoop2.4/lib/spark-assembly-1.3.0-hadoop2.4.0.jar \
  --master "$MASTER" --executor-memory 8G --num-executors 10 \
  --driver-class-path ./target/svd-benchmark-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  ./target/svd-benchmark-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  --path "$OUT_PATH" --nRows "$N_ROWS" \
  --nCols "$N_COLS" --fracNonZero "$FRAC_NON" --blockSize "$BLOCK_SIZE"

