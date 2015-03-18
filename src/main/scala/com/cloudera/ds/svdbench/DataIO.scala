package com.cloudera.ds.svdbench

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.hadoop.io.{IntWritable, NullWritable}
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.mahout.math.{VectorWritable, DenseVector => DenseMahoutVector, Vector => MahoutVector}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, Vectors, Vector => SparkVector}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

object DataIO {
  /** Convert a Mahout vector to a Spark vector*/
  def mahoutToSparkVec(vec: VectorWritable): SparkVector = {
    val inVec: Array[MahoutVector.Element] = vec.get().nonZeroes.asScala.toArray[MahoutVector
    .Element]
    Vectors.sparse(inVec.size, inVec.map((elem: MahoutVector.Element) => (elem.index, elem.get)))
  }

  /** Convert a Spark vector to a Mahout vector*/
  def sparkToWritableVec(vec: SparkVector): VectorWritable = {
    new VectorWritable(new DenseMahoutVector(vec.toArray))
  }

  /** Reads in a sequence file of (IntWritable, VectorWritable) and returns a RowMatrix. */
  def readMahoutMatrix(path: String, sc: SparkContext): RowMatrix
  = {
    val indexedRows: RDD[SparkVector] = {sc.sequenceFile[IntWritable,
      VectorWritable](path).values.map((rowVec: VectorWritable) => mahoutToSparkVec(rowVec))}
    new RowMatrix(indexedRows)
  }

  /** Writes a matrix out in a Mahout readable format. Specifically as a sequence file of
    * (IntWritable, VectorWritable).
    * */
  def writeMahoutMatrix(path: String, matrix: RDD[(IntWritable, VectorWritable)]) = {
    matrix.saveAsNewAPIHadoopFile(path, classOf[IntWritable], classOf[VectorWritable],
      classOf[SequenceFileOutputFormat[_, _]])
  }

  /** Writes a SparkRowMatrix to a seq file of vector writables. */
  def writeSparkRowMatrix(path: String, matrix: RowMatrix) = {
    val mahoutMat: RDD[(NullWritable, VectorWritable)] = matrix.rows.map((vec: SparkVector)=>(null,
      sparkToWritableVec(vec)))
    mahoutMat.saveAsNewAPIHadoopFile(path, classOf[IntWritable], classOf[VectorWritable],
      classOf[SequenceFileOutputFormat[_, _]])
  }
  /** Writes a Spark matrix to a UTF-8 encoded csv file. */
  def writeSparkMatrix(path: String, matrix: Matrix) = {
    val colLength = matrix.numRows
    val csvMatrix = matrix.toArray.grouped(colLength).map(column => column.mkString("," +
      "")).mkString("\n")
    Files.write(Paths.get(path), csvMatrix.getBytes(StandardCharsets.UTF_8))
  }

  /** Writes a spark vector to a UTF-8 encoded csv file. */
  def writeSparkVector(path: String, vector: SparkVector) = {
    Files.write(Paths.get(path), vector.toArray.mkString(",").getBytes(StandardCharsets.UTF_8))
  }
}
