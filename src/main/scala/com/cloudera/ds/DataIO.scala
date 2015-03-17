package com.cloudera.ds

import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.mahout.math.{VectorWritable, SequentialAccessSparseVector}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object DataIO {
  val blockSize = 1000

  /** Returns of a sparse vector of not more than nNonZero randomly selected elements. This vector
    * may have less than that many non zero element due to collisions. */
  def makeRandomSparseVec(size: Int, nNonZero: Int): SequentialAccessSparseVector = {
    val vec = new SequentialAccessSparseVector(size)

    val dataGen: RandomDataGenerator = {
      val gen = new RandomDataGenerator()
      gen.reSeed(2000)
      gen
    }
    for (nsample <- 1 to nNonZero) {
      vec.setQuick(dataGen.nextInt(0, size), 1)
    }
    vec
  }

  /** nRows is rounded up to the nearest thousand*/
  def generateSparseMatrix(nRows: Int, nCols: Int, nonZero: Int, rowBlockSize: Int = blockSize,
                           sc: SparkContext): RDD[(IntWritable, VectorWritable)] = {
    val rowBlockIndex = Array.range(0, nRows, blockSize)

    val rowIndices: RDD[Int] = sc.parallelize(rowBlockIndex)
      .flatMap(blockIdx => Array.range(blockIdx, blockIdx + blockSize))
    val matrix = rowIndices.map(rowIdx => (new IntWritable(rowIdx),
      new VectorWritable(makeRandomSparseVec(nCols, nonZero))))
    matrix
  }

  def readMatrix(path: String, sc: SparkContext): RDD[(IntWritable, VectorWritable)]
  = {
    sc.sequenceFile[IntWritable, VectorWritable](path)
  }

  def writeMatrix(path: String, matrix: RDD[(IntWritable, VectorWritable)]) = {
    matrix.saveAsNewAPIHadoopFile(path, classOf[IntWritable], classOf[VectorWritable],
      classOf[SequenceFileOutputFormat[_, _]])
  }
}
