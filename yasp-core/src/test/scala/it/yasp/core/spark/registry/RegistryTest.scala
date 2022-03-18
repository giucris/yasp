package it.yasp.core.spark.registry

import it.yasp.core.spark.registry.Registry.DefaultRegistry
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover}

@DoNotDiscover
class RegistryTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {

  override protected def beforeAll(): Unit =
    super.beforeAll()

  override protected def afterAll(): Unit =
    super.afterAll()

  test("register") {
    val expected = spark.createDataset(Seq(Row("h1", "h2", "h3"), Row("a", "b", "c")))(
      RowEncoder(
        StructType(
          Seq(
            StructField("_c0", StringType, nullable = true),
            StructField("_c1", StringType, nullable = true),
            StructField("_c2", StringType, nullable = true)
          )
        )
      )
    )
    new DefaultRegistry(spark).register(expected, "test_table")
    val actual   = spark.table("test_table")
    assertDatasetEquals(actual, expected)
  }

  test("retrieve") {
    val expected = spark.createDataset(Seq(Row("h1", "h2", "h3"), Row("a", "b", "c")))(
      RowEncoder(
        StructType(
          Seq(
            StructField("_c0", StringType, nullable = true),
            StructField("_c1", StringType, nullable = true),
            StructField("_c2", StringType, nullable = true)
          )
        )
      )
    )
    expected.createTempView("test_table_2")

    val actual = new DefaultRegistry(spark).retrieve("test_table_2")
    assertDatasetEquals(actual, expected)
  }

}
