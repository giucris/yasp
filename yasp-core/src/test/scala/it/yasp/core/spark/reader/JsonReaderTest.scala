package it.yasp.core.spark.reader

import it.yasp.core.spark.model.Source.Json
import it.yasp.core.spark.reader.Reader.JsonReader
import it.yasp.testkit.SparkTestSuite
import it.yasp.testkit.TestUtils._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class JsonReaderTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {

  private val workspace = "yasp-core/src/test/resources/JsonReaderTest"

  override protected def beforeAll(): Unit = {
    cleanFolder(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    cleanFolder(workspace)
    super.afterAll()
  }

  val reader = new JsonReader(spark)

  test("read") {
    createFile(
      filePath = s"$workspace/input1/file1.json",
      rows = Seq(
        "{\"a\":\"file1\",\"b\":1}",
        "{\"a\":\"file1\",\"c\":2}"
      )
    )
    createFile(
      filePath = s"$workspace/input1/file2.json",
      rows = Seq(
        "{\"a\":\"file2\",\"b\":3}",
        "{\"a\":\"file2\",\"c\":4}"
      )
    )

    val expected = spark.createDataset(
      Seq(
        Row("file1", 1L, null),
        Row("file1", null, 2L),
        Row("file2", 3L, null),
        Row("file2", null, 4L)
      )
    )(
      RowEncoder(
        StructType(
          Seq(
            StructField("a", StringType),
            StructField("b", LongType),
            StructField("c", LongType)
          )
        )
      )
    )
    val actual   = reader.read(Json(s"$workspace/input1/"))
    assertDatasetEquals(actual, expected)
  }

  test("read with schema") {
    createFile(
      filePath = s"$workspace/input2/file1.json",
      rows = Seq(
        "{\"a\":\"file1\",\"b\":1}"
      )
    )
    createFile(
      filePath = s"$workspace/input2/file2.json",
      rows = Seq(
        "{\"a\":\"file2\",\"b\":2}"
      )
    )
    val expected = spark.createDataset(
      Seq(
        Row("file1", 1L, null, null),
        Row("file2", 2L, null, null)
      )
    )(
      RowEncoder(
        StructType(
          Seq(
            StructField("a", StringType),
            StructField("b", LongType),
            StructField("c", LongType),
            StructField("d", StringType)
          )
        )
      )
    )
    val actual   = reader.read(
      Json(
        json = s"$workspace/input2/",
        schema = Some("a STRING, b LONG, c LONG, d STRING")
      )
    )
    assertDatasetEquals(actual, expected)
  }

}
