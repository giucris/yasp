package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest.Parquet
import it.yasp.core.spark.writer.Writer.ParquetWriter
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ParquetWriterTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {

  val writer           = new ParquetWriter()
  val df: Dataset[Row] = spark.createDataset(Seq(Row("a", "b", "c")))(
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
    writer.write(df, Parquet(s"$workspace/parquet1/"))
    val actual = spark.read.parquet(s"$workspace/parquet1/")
    assertDatasetEquals(actual, df)
  }

  test("write with partitionBy") {
    writer.write(df, Parquet(s"$workspace/parquet2/", partitionBy = Seq("h1", "h2")))

    val actual   = spark.read
      .option("basePath", s"$workspace/parquet2/")
      .parquet(s"$workspace/parquet2/h1=b/h2=c/")
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

    assertDatasetEquals(actual, expected)
  }

  test("write with append mode") {
    writer.write(df, Parquet(s"$workspace/parquet3/", mode = Some("append")))
    writer.write(df, Parquet(s"$workspace/parquet3/", mode = Some("append")))

    val actual   = spark.read.parquet(s"$workspace/parquet3/")
    val expected = df.union(df)
    assertDatasetEquals(actual, expected)
  }

  test("write with overwrite mode") {
    writer.write(df, Parquet(s"$workspace/parquet4/", mode = Some("overwrite")))
    writer.write(df, Parquet(s"$workspace/parquet4/", mode = Some("overwrite")))

    val actual = spark.read.parquet(s"$workspace/parquet4/")
    assertDatasetEquals(actual, df)
  }

}
