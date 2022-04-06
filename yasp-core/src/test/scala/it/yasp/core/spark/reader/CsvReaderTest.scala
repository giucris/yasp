package it.yasp.core.spark.reader

import it.yasp.core.spark.model.Source.Csv
import it.yasp.core.spark.reader.Reader.CsvReader
import it.yasp.testkit.SparkTestSuite
import it.yasp.testkit.TestUtils._
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
    cleanFolder(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    cleanFolder(workspace)
    super.afterAll()
  }

  test("read without header") {
    createFile(s"$workspace/input1/file1.csv", Seq("h1,h2,h3", "a,b,c"))
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
    val actual   = new CsvReader(spark).read(Csv(s"$workspace/input1/"))
    assertDatasetEquals(actual, expected)
  }

  test("read with header") {
    createFile(s"$workspace/input2/file1.csv", Seq("h1,h2,h3", "a,b,c"))
    createFile(s"$workspace/input2/file2.csv", Seq("h1,h2,h3", "d,e,f"))
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
      Csv(s"$workspace/input2/", options = Map("header" -> "true"))
    )
    assertDatasetEquals(actual, expected)
  }

  test("read with header and custom sep") {
    createFile(s"$workspace/input3/file1.csv", Seq("h1|h2|h3", "a|b|c"))
    createFile(s"$workspace/input3/file2.csv", Seq("h1|h2|h3", "d|e|f"))
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
      Csv(s"$workspace/input3/", options = Map("header" -> "true", "sep" -> "|"))
    )
    assertDatasetEquals(actual, expected)
  }

  test("read with header custom sep and schema") {
    createFile(s"$workspace/input4/file1.csv", Seq("h1|h2|h3", "1|b|c"))
    createFile(s"$workspace/input4/file2.csv", Seq("h1|h2|h3", "2|e|f"))
    val expected = spark.createDataset(Seq(Row(1, "b", "c"), Row(2, "e", "f")))(
      RowEncoder(
        StructType(
          Seq(
            StructField("h1", IntegerType, nullable = true),
            StructField("h2", StringType, nullable = true),
            StructField("h3", StringType, nullable = true)
          )
        )
      )
    )

    val actual = new CsvReader(spark).read(
      Csv(
        path = s"$workspace/input4/",
        options = Map("header" -> "true", "sep" -> "|", "schema" -> "h1 INT, h2 STRING, h3 STRING")
      )
    )
    assertDatasetEquals(actual, expected)
  }

  test("read with header custom sep schema and corrupt record") {
    createFile(s"$workspace/input5/file1.csv", Seq("h1|h2|h3", "1|b|c"))
    createFile(s"$workspace/input5/file2.csv", Seq("h1|h2|h3", "x|x|x"))
    val expected = spark.createDataset(Seq(Row(1, "b", "c", null), Row(null, null, null, "x|x|x")))(
      RowEncoder(
        StructType(
          Seq(
            StructField("h1", IntegerType, nullable = true),
            StructField("h2", StringType, nullable = true),
            StructField("h3", StringType, nullable = true),
            StructField("_corrupt_record", StringType, nullable = true)
          )
        )
      )
    )

    val actual = new CsvReader(spark).read(
      Csv(
        path = s"$workspace/input5/",
        options = Map(
          "header" -> "true",
          "sep"    -> "|",
          "schema" -> "h1 INT, h2 STRING, h3 STRING,_corrupt_record STRING"
        )
      )
    )
    assertDatasetEquals(actual, expected)
  }

}
