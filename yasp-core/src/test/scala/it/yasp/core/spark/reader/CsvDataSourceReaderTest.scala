package it.yasp.core.spark.reader

import it.yasp.core.spark.model.DataSource.Csv
import it.yasp.core.spark.reader.DataSourceReader.CsvDataSourceReader
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class CsvDataSourceReaderTest extends AnyFunSuite with SparkTestSuite {

  private val workspace = "yasp-core/src/test/resources/CsvReaderTest"
  val reader            = new CsvDataSourceReader(spark)

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  test("read single file without header") {
    TestUtils.createFile(s"$workspace/singleCsv/file1.csv", Seq("h1,h2,h3", "a,b,c"))
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
    val actual   = reader.read(
      Csv(
        Seq(s"$workspace/singleCsv/file1.csv"),
        header = false,
        separator = ","
      )
    )
    assertDatasetEquals(actual, expected)
  }

  test("read single file with header") {
    TestUtils.createFile(s"$workspace/singleCsv/file2.csv", Seq("h1,h2,h3", "a,b,c"))
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
    val actual   = reader.read(
      Csv(
        Seq(s"$workspace/singleCsv/file2.csv"),
        header = true,
        separator = ","
      )
    )
    assertDatasetEquals(actual, expected)
  }

  test("read single file with header and custom separator") {
    TestUtils.createFile(s"$workspace/singleCsvCustomSep/file1.csv", Seq("h1|h2|h3", "a|b|c"))
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
    val actual   = reader.read(
      Csv(
        Seq(s"$workspace/singleCsvCustomSep/file1.csv"),
        header = true,
        separator = "|"
      )
    )
    assertDatasetEquals(actual, expected)
  }

  test("read multiple file with header") {
    TestUtils.createFile(s"$workspace/multipleCsv/file1.csv", Seq("h1,h2,h3", "a,b,c"))
    TestUtils.createFile(s"$workspace/multipleCsv/file2.csv", Seq("h1,h2,h3", "d,e,f"))
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
    val actual   = reader.read(
      Csv(
        Seq(s"$workspace/multipleCsv/file1.csv", s"$workspace/multipleCsv/file2.csv"),
        header = true,
        separator = ","
      )
    )
    assertDatasetEquals(actual, expected)
  }

  test("read multiple file with header and custom separator") {
    TestUtils.createFile(s"$workspace/multipleCsvCustomSep/file1.csv", Seq("h1|h2|h3", "a|b|c"))
    TestUtils.createFile(s"$workspace/multipleCsvCustomSep/file2.csv", Seq("h1|h2|h3", "d|e|f"))
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
    val actual   = reader.read(
      Csv(
        paths = Seq(s"$workspace/multipleCsvCustomSep/"),
        header = true,
        separator = "|"
      )
    )
    assertDatasetEquals(actual, expected)
  }
}
