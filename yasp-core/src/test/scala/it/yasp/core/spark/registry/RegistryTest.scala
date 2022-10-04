package it.yasp.core.spark.registry

import it.yasp.core.spark.err.YaspCoreError.{RegisterTableError, RetrieveTableError}
import it.yasp.core.spark.registry.Registry.DefaultRegistry
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class RegistryTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {
  val registry: DefaultRegistry = new DefaultRegistry(spark)

  val baseDf: Dataset[Row] = spark.createDataset(Seq(Row("h1", "h2", "h3"), Row("a", "b", "c")))(
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

  override protected def beforeAll(): Unit =
    super.beforeAll()

  override protected def afterAll(): Unit =
    super.afterAll()

  test("register") {
    registry.register(baseDf, "register_table_1")
    val actual = spark.table("register_table_1")
    assertDatasetEquals(actual, baseDf)
  }

  test("register return RegisterTableError") {
    registry.register(baseDf, "register_table_2")
    val actual = registry.register(baseDf, "register_table_2")
    assert(actual.left.getOrElse(fail()).isInstanceOf[RegisterTableError])
  }

  test("retrieve") {
    baseDf.createTempView("retrieve_table_1")
    val actual = registry.retrieve("retrieve_table_1")
    assertDatasetEquals(actual.getOrElse(fail()), baseDf)
  }

  test("retrieve return RetrieveTableError") {
    baseDf.createTempView("retrieve_table_2")
    val actual = registry.retrieve("retrieve_table_x")
    assert(actual.left.getOrElse(fail()).isInstanceOf[RetrieveTableError])
  }

}
