package it.yasp.core.spark.reader

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
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  test("read single file without header") {
    TestUtils.createFile(s"$workspace/read1/file1.csv", Seq("h1,h2,h3", "a,b,c"))
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
    val actual   =
      new CsvReader(spark).read(s"$workspace/read1/file1.csv", header = false, separator = ",")
    assertDatasetEquals(actual, expected)
  }

  test("read single file with header") {
    TestUtils.createFile(s"$workspace/read2/file1.csv", Seq("h1,h2,h3", "a,b,c"))
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
    val actual   =
      new CsvReader(spark).read(s"$workspace/read2/file1.csv", header = true, separator = ",")
    assertDatasetEquals(actual, expected)
  }

  test("read single file without header and custom separator") {
    TestUtils.createFile(s"$workspace/read3/file1.csv", Seq("h1|h2|h3", "a|b|c"))
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
    val actual   =
      new CsvReader(spark).read(s"$workspace/read3/file1.csv", header = false, separator = "|")
    assertDatasetEquals(actual, expected)
  }

  test("read single file with header and custom separator") {
    TestUtils.createFile(s"$workspace/read4/file1.csv", Seq("h1|h2|h3", "a|b|c"))
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
    val actual   =
      new CsvReader(spark).read(s"$workspace/read4/file1.csv", header = true, separator = "|")
    assertDatasetEquals(actual, expected)
  }

  test("read multiple file without header") {
    TestUtils.createFile(s"$workspace/read5/file1.csv", Seq("h1,h2,h3", "a,b,c"))
    TestUtils.createFile(s"$workspace/read5/file2.csv", Seq("h1,h2,h3", "d,e,f"))

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
    val actual   = new CsvReader(spark).read(s"$workspace/read5/", header = false, separator = ",")
    assertDatasetEquals(actual, expected)
  }

  test("read multiple file with header") {
    TestUtils.createFile(s"$workspace/read6/file1.csv", Seq("h1,h2,h3", "a,b,c"))
    TestUtils.createFile(s"$workspace/read6/file2.csv", Seq("h1,h2,h3", "d,e,f"))
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
    val actual   = new CsvReader(spark).read(s"$workspace/read6/", header = true, separator = ",")
    assertDatasetEquals(actual, expected)
  }

}
