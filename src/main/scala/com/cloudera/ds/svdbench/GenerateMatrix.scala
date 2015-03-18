package com.cloudera.ds.svdbench

import com.quantifind.sumac.{ArgMain, FieldArgs}
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
  def configure(master: String): SparkConf = {
    val conf = new SparkConf()
    conf.setMaster(master)
    conf
  }

  def main(args: GenMatrixArgs): Unit = {
    val sc = new SparkContext(configure(args.master))
    val matrix = DataIO.generateSparseMatrix(args.nRows, args.nCols, args.fracNonZero, args.blockSize,
      sc)
    DataIO.writeMatrix(args.path, matrix)
  }

}
