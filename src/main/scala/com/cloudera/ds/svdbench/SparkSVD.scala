package com.cloudera.ds.svdbench

import com.quantifind.sumac.{ArgMain, FieldArgs}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition}
import org.apache.spark.{SparkConf, SparkContext}

class SVDArgs extends FieldArgs {
  var inPath = ""
  var outPath = ""
  var master = "yarn-client"
}

object SparkSVD extends ArgMain[SVDArgs] {
  def configure(master: String): SparkConf = {
    val conf = new SparkConf()
    conf.setMaster(master)
    conf
  }

  def main(args: SVDArgs): Unit = {
    val sc = new SparkContext(configure(args.master))
    val matrix: RowMatrix = DataIO.readMatrix(args.inPath, sc)
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = matrix.computeSVD(20, computeU = true)
  }
}
