package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest.{Csv, Jdbc, Json, Parquet}
import it.yasp.core.spark.writer.Writer.DestWriter
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.DriverManager.registerDriver
import java.util.Properties

class WriterTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {
  registerDriver(new org.h2.Driver)

  val writer                   = new DestWriter()
  private val workspace        = "yasp-core/src/test/resources/WriterTest"
  private val connUrl1: String = "jdbc:h2:mem:dbx"

  private val df: Dataset[Row] = spark.createDataset(Seq(Row("a", "b", "c")))(
    RowEncoder(
      StructType(
        Seq(
          StructField("h0", StringType, nullable = true),
          StructField("h1", StringType, nullable = true),
          StructField("h2", StringType, nullable = true)
        )
      )
    )
  )

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  test("write parquet") {
    writer.write(df, Parquet(s"$workspace/parquet1/"))
    val actual = spark.read.parquet(s"$workspace/parquet1/")
    assertDatasetEquals(actual, df)
  }

  test("write csv") {
    writer.write(df, Csv(s"$workspace/csv1/", Map("header" -> "true")))
    val actual = spark.read.options(Map("header" -> "true")).csv(s"$workspace/csv1/")
    assertDatasetEquals(actual, df)
  }

  test("write json") {
    writer.write(df, Json(s"$workspace/json1/"))
    val actual = spark.read.csv(s"$workspace/json1/")
    assertDatasetEquals(actual, df)
  }

  test("write jdbc") {
    writer.write(df, Jdbc(connUrl1, options = Map("dbTable" -> "my_table")))
    val actual = spark.read.jdbc(connUrl1, "my_table", new Properties())
    assertDatasetEquals(actual, df)
  }

}
