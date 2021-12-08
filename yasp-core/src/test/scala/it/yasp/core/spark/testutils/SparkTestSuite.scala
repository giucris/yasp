package it.yasp.core.spark.testutils

import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.{Assertion, Suite}

trait SparkTestSuite extends SharedSparkSession {
  this: Suite =>

  def assertDatasetEquals(actual: Dataset[Row], expected: Dataset[Row]): Assertion = {
    val actualSchema      = actual.schema
    val expectedSchema    = expected.schema
    val actualCollected   = actual.collect()
    val expectedCollected = expected.collect()
    assert(actualSchema == expectedSchema)
    assert(actualCollected sameElements expectedCollected)
  }

}
