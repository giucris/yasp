package it.yasp.core.spark.testutils

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SharedSparkSession extends BeforeAndAfterAll {
  this: Suite =>

  var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("testSession")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }
}
