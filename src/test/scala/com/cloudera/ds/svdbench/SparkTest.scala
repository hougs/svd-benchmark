package com.cloudera.ds.svdbench


import org.apache.spark.SparkContext
import org.scalatest.FunSuite

object SparkTest extends org.scalatest.Tag("com.qf.test.tags.SparkTest")

trait SparkTestUtils extends FunSuite {
  var sc: SparkContext = _

  /**
   * convenience method for tests that use spark.  Creates a local spark context, and cleans
   * it up even if your test fails.  Also marks the test with the tag SparkTest, so you can
   * turn it off
   *
   * By default, it turn off spark logging, b/c it just clutters up the test output.  However,
   * when you are actively debugging one test, you may want to turn the logs on
   *
   * @param name the name of the test
   * @param silenceSpark true to turn off spark logging
   */
  def sparkTest(name: String, silenceSpark : Boolean = true)(body: => Unit) {
    test(name, SparkTest){
      sc = new SparkContext("local[4]", name)
      try {
        body
      }
      finally {
        sc.stop
        sc = null
        // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
        System.clearProperty("spark.master.port")
      }
    }
  }
}