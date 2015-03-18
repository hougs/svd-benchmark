package com.cloudera.ds.svdbench

import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.mahout.math.{VectorWritable, SequentialAccessSparseVector,
Vector => MahoutVector}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector => SparkVector, Vectors}
import scala.collection.JavaConverters._

object DataIO {

  /** Probabilistically selects approx fracNonZero*size elements of a vector of size `size` to be
   non zero. */
  def makeRandomSparseVec(size: Int, fracNonZero: Double): SequentialAccessSparseVector = {
    val vec = new SequentialAccessSparseVector(size)

    val dataGen: RandomDataGenerator = {
      val gen = new RandomDataGenerator()
      gen.reSeed(2000)
      gen
    }
    for (nsample <- 1 to size) {
      if (dataGen.nextUniform(0, 1) < fracNonZero){
        vec.setQuick(nsample, 1)
      }
    }
    vec
  }

  /** nRows is rounded up to the nearest thousand*/
  def generateSparseMatrix(nRows: Int, nCols: Int, fracNonZero: Double, rowBlockSize: Int,
                           sc: SparkContext): RDD[(IntWritable, VectorWritable)] = {
    val rowBlockIndex = Array.range(0, nRows, rowBlockSize)

    val rowIndices: RDD[Int] = sc.parallelize(rowBlockIndex)
      .flatMap(blockIdx => Array.range(blockIdx, blockIdx + rowBlockSize))
    val matrix = rowIndices.map(rowIdx => (new IntWritable(rowIdx),
      new VectorWritable(makeRandomSparseVec(nCols, fracNonZero))))
    matrix
  }
  /** Convert a Mahout vector to a spark vector*/
  def vecTovec(vec: VectorWritable): SparkVector = {
    val inVec: Array[MahoutVector.Element] = vec.get().nonZeroes.asScala.toArray[MahoutVector
    .Element]
    Vectors.sparse(inVec.size, inVec.map((elem: MahoutVector.Element) => (elem.index, elem.get)))
  }

  def readMatrix(path: String, sc: SparkContext): RowMatrix
  = {
    val indexedRows: RDD[SparkVector] = {sc.sequenceFile[IntWritable,
      VectorWritable](path).values.map((rowVec: VectorWritable) => vecTovec(rowVec))}
    new RowMatrix(indexedRows)
  }

  def writeMatrix(path: String, matrix: RDD[(IntWritable, VectorWritable)]) = {
    matrix.saveAsNewAPIHadoopFile(path, classOf[IntWritable], classOf[VectorWritable],
      classOf[SequenceFileOutputFormat[_, _]])
  }
}
