package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest.Parquet
import it.yasp.core.spark.writer.Writer.ParquetWriter
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class WriterTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {

  private val workspace = "yasp-core/src/test/resources/WriterTest"

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  test("write parquet") {
    new ParquetWriter().write(
      spark.createDataset(Seq(Row("a", "b", "c")))(
        RowEncoder(
          StructType(
            Seq(
              StructField("h0", StringType, nullable = true),
              StructField("h1", StringType, nullable = true),
              StructField("h2", StringType, nullable = true)
            )
          )
        )
      ),
      Parquet(s"$workspace/parquet1/")
    )

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
    val actual   = spark.read.parquet(s"$workspace/parquet1/")

    assertDatasetEquals(actual, expected)
  }

  test("write parquet with partitionBy") {
    new ParquetWriter().write(
      spark.createDataset(Seq(Row("a", "b", "c")))(
        RowEncoder(
          StructType(
            Seq(
              StructField("h0", StringType, nullable = true),
              StructField("h1", StringType, nullable = true),
              StructField("h2", StringType, nullable = true)
            )
          )
        )
      ),
      Parquet(s"$workspace/parquet2/", partitionBy = Seq("h0", "h1"))
    )

    val expected = spark.createDataset(Seq(Row("c")))(
      RowEncoder(StructType(Seq(StructField("h2", StringType, nullable = true))))
    )
    val actual   = spark.read.parquet(s"$workspace/parquet2/h0=a/h1=b/")

    assertDatasetEquals(actual, expected)
  }

  test("write json") {}
}
