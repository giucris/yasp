package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest.{Csv, Parquet}
import it.yasp.core.spark.writer.Writer.writer
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover}

@DoNotDiscover
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

  test("write csv") {
    writer[Csv].write(
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
      Csv(s"$workspace/csv1/", options = Map("header" -> "true"))
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
    val actual   = spark.read.option("header", "true").csv(s"$workspace/csv1/")

    assertDatasetEquals(actual, expected)
  }

  test("write csv with partitionBy") {
    writer[Csv].write(
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
      Csv(s"$workspace/csv1/", partitionBy = Seq("h0"), options = Map("header" -> "true"))
    )

    val expected = spark.createDataset(Seq(Row("b", "c")))(
      RowEncoder(
        StructType(
          Seq(
            StructField("h1", StringType, nullable = true),
            StructField("h2", StringType, nullable = true)
          )
        )
      )
    )
    val actual   = spark.read.option("header", "true").csv(s"$workspace/csv1/h0=a/")

    assertDatasetEquals(actual, expected)
  }

  test("write parquet") {
    writer[Parquet].write(
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
    writer[Parquet].write(
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

  test("write json"){

  }
}
