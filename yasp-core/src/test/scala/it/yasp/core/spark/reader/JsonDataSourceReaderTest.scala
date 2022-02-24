package it.yasp.core.spark.reader

import it.yasp.core.spark.model.DataSource.Json
import it.yasp.core.spark.reader.DataSourceReader.JsonDataSourceReader
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class JsonDataSourceReaderTest extends AnyFunSuite with SparkTestSuite {

  private val workspace = "yasp-core/src/test/resources/JsonReaderTest"
  val reader = new JsonDataSourceReader(spark)

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  test("read single json file") {
    TestUtils.createFile(
      filePath = s"$workspace/json/json1.json",
      rows = Seq(
        "{\"a\":\"abc\",\"b\":1}",
        "{\"a\":\"def\",\"c\":2}"
      )
    )
    val expected = spark.createDataset(Seq(Row("abc", 1L, null), Row("def", null, 2L)))(
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

    val actual = reader.read(Json(Seq(s"$workspace/json/json1.json")))

    assertDatasetEquals(actual, expected)
  }

  test("read multiple json file") {
    TestUtils.createFile(
      filePath = s"$workspace/jsons/json1.json",
      rows = Seq(
        "{\"a\":\"file1\",\"b\":1}",
        "{\"a\":\"file1\",\"c\":2}"
      )
    )
    TestUtils.createFile(
      filePath = s"$workspace/jsons/json2.json",
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
    val actual   = reader.read(Json(Seq(s"$workspace/jsons/")))
    assertDatasetEquals(actual, expected)
  }
}
