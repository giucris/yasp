package it.yasp.core.spark.reader

import it.yasp.core.spark.testutils.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class ParquetReaderTest extends AnyFunSuite with SparkTestSuite {

  private val workspace = "yasp-core/src/test/resources/ParquetReaderTest"

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  test("read") {
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
    val actual   = new ParquetReader(spark).read(s"$workspace/parquet1/")
    assertDatasetEquals(actual, expected)
  }

}
