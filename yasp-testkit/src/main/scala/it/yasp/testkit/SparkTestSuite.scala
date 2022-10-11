package it.yasp.testkit

import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.{Assertion, Suite}

/** SparkTestSuite a mixin for [[Suite]] to test spark code
  *
  * Provide a shared spark session using the [[SharedSparkSession]] trait and an assertDatasetEquals
  * method to test equality among two dataset.
  *
  * Example usage:
  * {{{
  *   class MySparkTest extends AnyFunSuite with SparkTestSuite {
  *
  *     test("myTest"){
  *       val actual = spark.read.csv("myCsvFile")
  *       val expected = ....
  *       assertDatasetEquals(actual,expected)
  *     }
  *
  *   }
  * }}}
  */
trait SparkTestSuite extends SharedSparkSession {
  this: Suite =>

  /** Assertion method for [[Dataset]] of [[Row]]
    *
    * Dataset are equal if schema are equals and rows are equals.
    *
    * During the schema evaluation the metadata are excluded
    *
    * @param actual:
    *   the actual [[Dataset]]
    * @param expected:
    *   the expected [[Dataset]]
    * @return
    *   [[Assertion]]
    */
  def assertDatasetEquals(actual: Dataset[Row], expected: Dataset[Row]): Assertion = {
    implicit val rowOrdering: Ordering[Row] = Ordering.by(r => r.mkString)

    val actualSchema      = actual.schema.map(_.copy(metadata = Metadata.empty))
    val expectedSchema    = expected.schema.map(_.copy(metadata = Metadata.empty))
    val actualCollected   = actual.collect()
    val expectedCollected = expected.collect()

    assert(actualSchema === expectedSchema)
    assert(actualCollected.sorted === expectedCollected.sorted)
  }

}
