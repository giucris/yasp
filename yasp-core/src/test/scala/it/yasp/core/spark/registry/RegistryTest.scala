package it.yasp.core.spark.registry

import it.yasp.core.spark.registry.Registry.DefaultRegistry
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class RegistryTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {

  override protected def beforeAll(): Unit =
    super.beforeAll()

  override protected def afterAll(): Unit =
    super.afterAll()

  val registry = new DefaultRegistry(spark)

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
    registry.register(expected, "test_table")
    assertDatasetEquals(spark.table("test_table"), expected)
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

    assertDatasetEquals(registry.retrieve("test_table_2"), expected)
  }

}
