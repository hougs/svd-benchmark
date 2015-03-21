package com.cloudera.ds.svdbench

import com.quantifind.sumac.{ArgMain, FieldArgs}
import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.hadoop.io.IntWritable
import org.apache.mahout.math.{RandomAccessSparseVector, VectorWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class GenMatrixArgs extends FieldArgs {
  var path: String = "hdfs://matrix"
  var master: String = "local"
  var nRows: Int = 10
  var nCols: Int = 10
  var fracNonZero: Double = 0.1
  var blockSize: Int = 2
}

object GenerateMatrix extends ArgMain[GenMatrixArgs] {
  /** Probabilistically selects approx fracNonZero*size elements of a vector of size `size` to be
   non zero. */
  def makeRandomSparseVec(size: Int, fracNonZero: Double): RandomAccessSparseVector = {
    val vec = new RandomAccessSparseVector(size)
    val dataGen: RandomDataGenerator = new RandomDataGenerator()

    for (nsample <- 0 until size) {
      if (dataGen.nextUniform(0, 1) < fracNonZero){
        vec.setQuick(nsample, 1.0)
      }
    }
    vec
  }

  /** nRows is rounded up to the nearest thousand*/
  def generateSparseMatrix(nRows: Int, nCols: Int, fracNonZero: Double, rowBlockSize: Int,
                           sc: SparkContext): RDD[(IntWritable, VectorWritable)] = {
    val rowBlockIndex = Array.range(0, nRows, rowBlockSize)
    val rowIndices: RDD[Int] = sc.parallelize(rowBlockIndex, numSlices = rowBlockIndex.size)
      .flatMap(blockIdx => Array.range(blockIdx, blockIdx + rowBlockSize))
    val matrix = rowIndices.map(rowIdx => (new IntWritable(rowIdx),
      new VectorWritable(makeRandomSparseVec(nCols, fracNonZero))))
    matrix
  }

  def configure(master: String): SparkConf = {
    val conf = new SparkConf()
    conf.setMaster(master)
    conf.setAppName("Generate Matrix. ")
    conf
  }

  def main(args: GenMatrixArgs): Unit = {
    val sc = new SparkContext(configure(args.master))
    val matrix = generateSparseMatrix(args.nRows, args.nCols, args.fracNonZero, args.blockSize,
      sc)
    DataIO.writeMahoutMatrix(args.path, matrix)
  }

}
