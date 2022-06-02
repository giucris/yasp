package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest.Parquet
import it.yasp.core.spark.writer.Writer.ParquetWriter
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover}

@DoNotDiscover
class ParquetWriterTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {

  private val workspace = "yasp-core/src/test/resources/ParquetWriterTest"

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  test("write") {
    val expected = spark.createDataset(Seq(Row("a", "b", "c")))(
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
    new ParquetWriter().write(expected, Parquet(s"$workspace/parquet1/"))
    val actual   = spark.read.parquet(s"$workspace/parquet1/")

    assertDatasetEquals(actual, expected)
  }

  test("write with partitionBy") {
    val expected = spark.createDataset(Seq(Row("a", "b", "c")))(
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

    new ParquetWriter()
      .write(expected, Parquet(s"$workspace/parquet2/", partitionBy = Seq("h1", "h2")))

    val actual = spark.read
      .option("basePath", s"$workspace/parquet2/")
      .parquet(s"$workspace/parquet2/h1=b/h2=c/")

    assertDatasetEquals(actual, expected)
  }
}
