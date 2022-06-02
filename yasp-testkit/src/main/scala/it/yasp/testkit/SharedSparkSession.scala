package it.yasp.testkit

import org.apache.spark.sql.SparkSession
import org.scalatest.Suite

/** A SharedSparkSession trait
  *
  * Provide a spark session for testing purpose. Setup the spark session on the beforeAll method and
  * close the spark session on the afterAll method
  */
trait SharedSparkSession {
  this: Suite =>
  val spark: SparkSession = SharedSparkSession.init
}

object SharedSparkSession {
  def init: SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("testSession")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }
}
