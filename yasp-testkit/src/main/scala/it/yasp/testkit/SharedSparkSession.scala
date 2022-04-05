package it.yasp.testkit

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** A SharedSparkSession trait
  *
  * Provide a spark session for testing purpose. Setup the spark session on the beforeAll method and
  * close the spark session on the afterAll method
  */
trait SharedSparkSession extends BeforeAndAfterAll {
  this: Suite =>

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  @transient var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("testSession")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    super.beforeAll()
  }

  override protected def afterAll(): Unit =
    super.afterAll()

}
