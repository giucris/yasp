package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest.{Custom, Format, HiveTable}
import it.yasp.core.spark.writer.Writer.DestWriter
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.{IntegerType, StringType}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Connection
import java.sql.DriverManager.{getConnection, registerDriver}
import java.util.Properties

class WriterTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {
  registerDriver(new org.h2.Driver)

  val writer            = new DestWriter()
  val workspace         = "yasp-core/src/test/resources/WriterTest"
  val dbConnUrl: String = "jdbc:h2:mem:dbw2"
  val conn1: Connection = getConnection(dbConnUrl)

  val expectedDf: Dataset[Row] = spark.createDataset(Seq(Row(1, "x"), Row(2, "y")))(
    RowEncoder(
      StructType(
        Seq(
          StructField("id", IntegerType, nullable = true),
          StructField("field1", StringType, nullable = true)
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
    writer.write(
      expectedDf,
      Format(
        format = "parquet",
        options = Map("path" -> s"$workspace/parquet1/")
      )
    )
    val actual = spark.read.parquet(s"$workspace/parquet1/")
    assertDatasetEquals(actual, expectedDf)
  }

  test("write csv") {
    writer.write(
      expectedDf,
      Format(
        format = "csv",
        options = Map("header" -> "true", "path" -> s"$workspace/csv1/")
      )
    )
    val actual = spark.read
      .schema("id INT, field1 STRING")
      .options(Map("header" -> "true"))
      .csv(s"$workspace/csv1/")
    assertDatasetEquals(actual, expectedDf)
  }

  test("write json") {
    writer.write(expectedDf, Format("json", options = Map("path" -> s"$workspace/json1/")))
    val actual = spark.read.schema("id INT, field1 STRING").json(s"$workspace/json1/")
    assertDatasetEquals(actual, expectedDf)
  }

  test("write jdbc") {
    writer.write(
      expectedDf,
      Format("jdbc", Map("url" -> dbConnUrl, "dbTable" -> "my_test_table"), None)
    )

    val actual = spark.read.jdbc(dbConnUrl, "my_test_table", new Properties())

    assertDatasetEquals(actual, expectedDf)
  }

  test("write hive table") {
    writer.write(
      expectedDf,
      HiveTable("wr_table_1")
    )
    val actual = spark.table("wr_table_1")

    assertDatasetEquals(actual, expectedDf)
  }

  test("write custom") {
    val res = writer.write(
      expectedDf,
      Custom("it.yasp.core.spark.plugin.MyTestWriterPlugin", None)
    )
    assert(res.isRight)
  }
}
