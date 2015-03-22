package com.cloudera.ds.svdbench

import com.quantifind.sumac.{ArgMain, FieldArgs}
import org.apache.commons.math3.analysis.function.Log
import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.hadoop.io.IntWritable
import org.apache.mahout.math.{SequentialAccessSparseVector, VectorWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class GenMatrixArgs extends FieldArgs {
  var path: String = "hdfs://matrix"
  var master: String = "yarn-client"
  var nRows: Int = 10
  var nCols: Int = 10
  var fracNonZero: Double = 0.1
  var nPartitions: Int = 2
}

object GenerateMatrix extends ArgMain[GenMatrixArgs] {
  /** Probabilistically selects approx fracNonZero*size elements of a vector of size `size` to be
   non zero. We generate the index of the next non-zero element by adding a random variable with
  a geometric distribution (ceiling of a sample from the exponential. )*/
  def makeRandomSparseVec(size: Int, fracNonZero: Double): SequentialAccessSparseVector = {
    val vec = new SequentialAccessSparseVector(size)
    val dataGen: RandomDataGenerator = new RandomDataGenerator()

    var idx = -1
    val log = new Log()
    while (idx < size) {
      idx += Math.ceil(dataGen.nextExponential(-1 /log.value(1.0-fracNonZero))).toInt
      if (idx > -1 && idx < size) {
        vec.setQuick(idx, 1.0)
      }
    }
    vec
  }

  /** nRows is rounded up to the nearest thousand*/
  def generateSparseMatrix(nRows: Int, nCols: Int, fracNonZero: Double,
                           nPartitions: Int, sc: SparkContext): RDD[(IntWritable,
    VectorWritable)] = {
    val rowBlockSize: Int = nRows / nPartitions
    val rowBlockIndex = Array.range(0, nRows, rowBlockSize)

    val rowBlocks: RDD[Int] = sc.parallelize(rowBlockIndex, nPartitions)
    val rows: RDD[Int] = rowBlocks.flatMap(blockIdx => Array.range(blockIdx, blockIdx + rowBlockSize))
    val matrix = rows.map(rowIdx => (new IntWritable(rowIdx),
      new VectorWritable(makeRandomSparseVec(nCols, fracNonZero))))
    matrix
  }

  def configure(master: String): SparkConf = {
    val conf = new SparkConf()
    conf.setMaster(master)
    conf.setAppName("Generate Matrix. ")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf
  }

  def main(args: GenMatrixArgs): Unit = {
    val sc = new SparkContext(configure(args.master))
    val matrix = generateSparseMatrix(args.nRows, args.nCols, args.fracNonZero,
      args.nPartitions, sc)
    DataIO.writeMahoutMatrix(args.path, matrix)
  }

}
