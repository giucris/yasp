package it.yasp.core.spark.reader

import it.yasp.core.spark.model.Source.Parquet
import it.yasp.core.spark.reader.DataSourceReader.ParquetDataSourceReader
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class ParquetSourceReaderTest extends AnyFunSuite with SparkTestSuite {

  private val workspace = "yasp-core/src/test/resources/ParquetReaderTest"

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  test("read single file") {
    spark
      .createDataset(Seq(Row("a", "b", "c")))(
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
      .write
      .parquet(s"$workspace/parquet1/")

    val expected = spark
      .createDataset(Seq(Row("a", "b", "c")))(
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
    val actual   = new ParquetDataSourceReader(spark).read(
      Parquet(Seq(s"$workspace/parquet1/"), mergeSchema = false)
    )
    assertDatasetEquals(actual, expected)
  }

  test("read multiple file") {
    spark
      .createDataset(Seq(Row("a", "b", "c")))(
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
      .write
      .parquet(s"$workspace/parquet2/")

    spark
      .createDataset(Seq(Row("d", "e", "f")))(
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
      .write
      .mode(SaveMode.Append)
      .parquet(s"$workspace/parquet2/")

    val expected = spark
      .createDataset(Seq(Row("a", "b", "c"), Row("d", "e", "f")))(
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
    val actual   = new ParquetDataSourceReader(spark).read(
      Parquet(Seq(s"$workspace/parquet2/"), mergeSchema = false)
    )
    assertDatasetEquals(actual, expected)
  }

  test("read multiple file with mergeSchema") {
    spark
      .createDataset(Seq(Row("a", "b", "c")))(
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
      .write
      .parquet(s"$workspace/parquet3/")

    spark
      .createDataset(Seq(Row("d", "e", "f", "g")))(
        RowEncoder(
          StructType(
            Seq(
              StructField("h0", StringType, nullable = true),
              StructField("h1", StringType, nullable = true),
              StructField("h2", StringType, nullable = true),
              StructField("h3", StringType, nullable = true)
            )
          )
        )
      )
      .write
      .mode(SaveMode.Append)
      .parquet(s"$workspace/parquet3/")

    val expected = spark
      .createDataset(Seq(Row("a", "b", "c", null), Row("d", "e", "f", "g")))(
        RowEncoder(
          StructType(
            Seq(
              StructField("h0", StringType, nullable = true),
              StructField("h1", StringType, nullable = true),
              StructField("h2", StringType, nullable = true),
              StructField("h3", StringType, nullable = true)
            )
          )
        )
      )
    val actual   = new ParquetDataSourceReader(spark).read(
      Parquet(Seq(s"$workspace/parquet3/"), mergeSchema = true)
    )
    assertDatasetEquals(actual, expected)
  }

}