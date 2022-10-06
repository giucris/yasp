package it.yasp.testkit

import org.apache.spark.sql.SparkSession
import org.scalatest.Suite

import java.nio.file.Files

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
    val warehouseDir = Files.createTempDirectory("spark-test-warehouse")
    val spark        = SparkSession
      .builder()
      .appName("test-session")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.warehouse.dir", warehouseDir.toUri.toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }
}
