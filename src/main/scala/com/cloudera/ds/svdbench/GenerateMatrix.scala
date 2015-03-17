package com.cloudera.ds.svdbench

import com.quantifind.sumac.{ArgMain, FieldArgs}
import org.apache.spark.{SparkConf, SparkContext}

class Arguments extends FieldArgs {
  var path: String = "hdfs://matrix"
  var master: String = "local"
  var nRows: Int = 10
  var nCols: Int = 10
  var nNonZero: Int = 3
  var blockSize: Int = 2
}

object GenerateMatrix extends ArgMain[Arguments] {
  def configure(master: String): SparkConf = {
    val conf = new SparkConf()
    conf.setMaster(master)
    conf
  }

  def main(args: Arguments): Unit = {
    val sc = new SparkContext(configure(args.master))
    val matrix = DataIO.generateSparseMatrix(args.nRows, args.nCols, args.nNonZero, args.blockSize,
      sc)
    DataIO.writeMatrix(args.path, matrix)
  }

}
