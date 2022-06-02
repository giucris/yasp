package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest.Csv
import it.yasp.core.spark.writer.Writer.CsvWriter
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class CsvWriterTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {
  private val workspace = "yasp-core/src/test/resources/CsvWriterTest"

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  val writer: CsvWriter = new CsvWriter()
  val df: Dataset[Row]  = spark.createDataset(Seq(Row("a", "b", "c"), Row("x", "y", "z")))(
    RowEncoder(
      StructType(
        Seq(
          StructField("h1", StringType, nullable = true),
          StructField("h2", StringType, nullable = true),
          StructField("h3", StringType, nullable = true)
        )
      )
    )
  )

  test("write") {
    writer.write(df, Csv(s"$workspace/output1/"))

    val actual   = spark.read.csv(s"$workspace/output1/")
    val expected = spark.createDataset(Seq(Row("a", "b", "c"), Row("x", "y", "z")))(
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
    assertDatasetEquals(actual, expected)
  }

  test("write with partitionBy") {
    writer.write(
      df,
      Csv(s"$workspace/output2/", options = Map("header" -> "true"), partitionBy = Seq("h1"))
    )

    val actual = spark.read
      .options(Map("basePath" -> s"$workspace/output2/", "header" -> "true"))
      .csv(s"$workspace/output2/h1=a/", s"$workspace/output2/h1=x/")

    val expected = spark.createDataset(Seq(Row("b", "c", "a"), Row("y", "z", "x")))(
      RowEncoder(
        StructType(
          Seq(
            StructField("h2", StringType, nullable = true),
            StructField("h3", StringType, nullable = true),
            StructField("h1", StringType, nullable = true)
          )
        )
      )
    )
    assertDatasetEquals(actual, expected)
  }

  test("write with append mode") {
    writer.write(df, Csv(s"$workspace/output3/", options = Map("header" -> "true"),mode=Some("append")))
    writer.write(df, Csv(s"$workspace/output3/", options = Map("header" -> "true"),mode=Some("append")))

    val actual   = spark.read.option("header","true").csv(s"$workspace/output3/")
    val expected = df.union(df)
    assertDatasetEquals(actual,expected)
  }

  test("write with overwrite mode") {
    writer.write(df, Csv(s"$workspace/output4/", options = Map("header" -> "true"),mode=Some("overwrite")))
    writer.write(df, Csv(s"$workspace/output4/", options = Map("header" -> "true"),mode=Some("overwrite")))

    val actual   = spark.read.option("header","true").csv(s"$workspace/output4/")
    assertDatasetEquals(actual,df)
  }
}
