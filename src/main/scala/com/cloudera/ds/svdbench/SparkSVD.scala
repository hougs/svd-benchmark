package com.cloudera.ds.svdbench

import com.quantifind.sumac.{ArgMain, FieldArgs}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition}
import org.apache.spark.{SparkConf, SparkContext}

class SVDArgs extends FieldArgs {
  var inPath = ""
  var outUPath = ""
  var outSPath = ""
  var outVPath = ""
  var master = "local"
}

object SparkSVD extends ArgMain[SVDArgs] {
  /** Configure our Spark Context. */
  def configure(master: String): SparkConf = {
    val conf = new SparkConf()
    conf.setMaster(master)
    conf.setAppName("Spark SVD")
    conf
  }

  def main(args: SVDArgs): Unit = {
    val sc = new SparkContext(configure(args.master))
    val matrix: RowMatrix = DataIO.readMahoutMatrix(args.inPath, sc)
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = matrix.computeSVD(20, computeU = true)
    // Write out svd to files.
    DataIO.writeSparkRowMatrix(args.outUPath, svd.U)
    DataIO.writeSparkVector(args.outSPath, svd.s)
    DataIO.writeSparkMatrix(args.outVPath, svd.V)
  }
}
