package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest.Format
import it.yasp.core.spark.writer.Writer.DestWriter
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Connection
import java.sql.DriverManager.{getConnection, registerDriver}
import java.util.Properties

class WriterTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {

  val writer                   = new DestWriter()
  private val workspace        = "yasp-core/src/test/resources/WriterTest"
  private val connUrl1: String = "jdbc:h2:mem:dbw"

  registerDriver(new org.h2.Driver)
  val conn1: Connection = getConnection(connUrl1)

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
    writer.write(df, Format("parquet", options = Map("path" -> s"$workspace/parquet1/")))
    val actual = spark.read.parquet(s"$workspace/parquet1/")
    assertDatasetEquals(actual, df)
  }

  test("write csv") {
    writer.write(
      df,
      Format("csv", options = Map("header" -> "true", "path" -> s"$workspace/csv1/"))
    )
    val actual = spark.read.options(Map("header" -> "true")).csv(s"$workspace/csv1/")
    assertDatasetEquals(actual, df)
  }

  test("write json") {
    writer.write(df, Format("json", options = Map("path" -> s"$workspace/json1/")))
    val actual = spark.read.json(s"$workspace/json1/")
    assertDatasetEquals(actual, df)
  }

  test("write jdbc") {
    writer.write(df, Format("jdbc", Map("url" -> connUrl1, "dbTable" -> "my_test_table"), None))

    val dfWithMetadata = df
      .withMetadata("h0", new MetadataBuilder().putLong("scale", 0).build())
      .withMetadata("h1", new MetadataBuilder().putLong("scale", 0).build())
      .withMetadata("h2", new MetadataBuilder().putLong("scale", 0).build())
    val actual         = spark.read.jdbc(connUrl1, "my_test_table", new Properties())
    assertDatasetEquals(actual, dfWithMetadata)
  }

}
