package it.yasp.core.spark.reader

import it.yasp.core.spark.model.Csv
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class CsvReaderTest extends AnyFunSuite with SparkTestSuite {

  private val workspace = "yasp-core/src/test/resources/CsvReaderTest"

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    TestUtils.createFile(s"$workspace/singleCsv/file1.csv", Seq("h1,h2,h3", "a,b,c"))
    TestUtils.createFile(s"$workspace/singleCsvCustomSep/file1.csv", Seq("h1|h2|h3", "a|b|c"))
    TestUtils.createFile(s"$workspace/multipleCsv/file1.csv", Seq("h1,h2,h3", "a,b,c"))
    TestUtils.createFile(s"$workspace/multipleCsv/file2.csv", Seq("h1,h2,h3", "d,e,f"))
    TestUtils.createFile(s"$workspace/multipleCsvCustomSep/file1.csv", Seq("h1|h2|h3", "a|b|c"))
    TestUtils.createFile(s"$workspace/multipleCsvCustomSep/file2.csv", Seq("h1|h2|h3", "d|e|f"))
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  test("read single file without header") {
    val expected = spark.createDataset(Seq(Row("h1", "h2", "h3"), Row("a", "b", "c")))(
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
    val actual   = new CsvReader(spark).read(
      Csv(
        s"$workspace/singleCsv/file1.csv",
        header = false,
        separator = ","
      )
    )
    assertDatasetEquals(actual, expected)
  }

  test("read single file with header") {
    val expected = spark.createDataset(Seq(Row("a", "b", "c")))(
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
    val actual   = new CsvReader(spark).read(
      Csv(
        s"$workspace/singleCsv/file1.csv",
        header = true,
        separator = ","
      )
    )
    assertDatasetEquals(actual, expected)
  }

  test("read single file without header and custom separator") {
    val expected = spark.createDataset(Seq(Row("h1", "h2", "h3"), Row("a", "b", "c")))(
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
    val actual   = new CsvReader(spark).read(
      Csv(
        s"$workspace/singleCsvCustomSep/file1.csv",
        header = false,
        separator = "|"
      )
    )
    assertDatasetEquals(actual, expected)
  }

  test("read single file with header and custom separator") {
    val expected = spark.createDataset(Seq(Row("a", "b", "c")))(
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
    val actual   = new CsvReader(spark).read(
      Csv(
        s"$workspace/singleCsvCustomSep/file1.csv",
        header = true,
        separator = "|"
      )
    )
    assertDatasetEquals(actual, expected)
  }

  test("read multiple file without header") {
    val expected = spark.createDataset(
      Seq(
        Row("h1", "h2", "h3"),
        Row("a", "b", "c"),
        Row("h1", "h2", "h3"),
        Row("d", "e", "f")
      )
    )(
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
    val actual   = new CsvReader(spark).read(
      Csv(
        s"$workspace/multipleCsv/",
        header = false,
        separator = ","
      )
    )
    assertDatasetEquals(actual, expected)
  }

  test("read multiple file with header") {
    val expected = spark.createDataset(Seq(Row("a", "b", "c"), Row("d", "e", "f")))(
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
    val actual   = new CsvReader(spark).read(
      Csv(
        s"$workspace/multipleCsv/",
        header = true,
        separator = ","
      )
    )
    assertDatasetEquals(actual, expected)
  }

}
