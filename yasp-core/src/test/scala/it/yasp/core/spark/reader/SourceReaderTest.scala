package it.yasp.core.spark.reader

import it.yasp.core.spark.model.Source._
import it.yasp.core.spark.reader.Reader._
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.LongType
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.h2.Driver
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Connection
import java.sql.DriverManager.{getConnection, registerDriver}

@DoNotDiscover
class SourceReaderTest extends AnyFunSuite with SparkTestSuite {

  private val workspace = "yasp-core/src/test/resources/SourceReaderTest"
  private val connUrl1: String = "jdbc:h2:mem:db3"

  registerDriver(new Driver)
  private val conn1: Connection = getConnection(connUrl1)

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    executeStatement(
      conn = conn1,
      stmt = "CREATE TABLE my_table (id INT,name VARCHAR(20),PRIMARY KEY (id))"
    )
    executeStatement(
      conn = conn1,
      stmt = "INSERT INTO my_table VALUES (1, 'name1'), (2,'name2'), (3,'name3'),(4,'name4')"
    )
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    executeStatement(conn1, "DROP TABLE my_table")
    executeStatement(conn1, "SHUTDOWN")
    super.afterAll()
  }

  test("read csv") {
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
    val actual   = new SourceReader(spark).read(
      Csv(
        Seq(s"$workspace/singleCsv/file1.csv"),
        header = false,
        separator = ","
      )
    )
    assertDatasetEquals(actual, expected)
  }

  test("read json") {
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

    val actual = new SourceReader(spark).read(Json(Seq(s"$workspace/json/json1.json")))

    assertDatasetEquals(actual, expected)
  }

  test("read parquet") {
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
    val actual   = new SourceReader(spark).read(
      Parquet(Seq(s"$workspace/parquet1/"), mergeSchema = false)
    )
    assertDatasetEquals(actual, expected)
  }

  test("read xml") {
    TestUtils.createFile(
      s"$workspace/xml/file.xml",
      Seq(
        "<root>",
        "<field1>value1</field1>",
        "<field2>value2</field2>",
        "</root>"
      )
    )

    val expected = spark.createDataset(Seq(Row("value1", "value2")))(
      RowEncoder(
        StructType(
          Seq(
            StructField("field1", StringType),
            StructField("field2", StringType)
          )
        )
      )
    )

    val actual = new SourceReader(spark).read(Xml(Seq(s"$workspace/xml/file.xml"), "root"))
    assertDatasetEquals(actual, expected)
  }

  test("read avro") {
    val expected = spark.createDataset(
      Seq(
        Row("data2", 1, 2, 3),
        Row("data1", 1, null, 2)
      )
    )(
      RowEncoder(
        StructType(
          Seq(
            StructField("d", StringType, nullable = true),
            StructField("a", IntegerType, nullable = true),
            StructField("b", IntegerType, nullable = true),
            StructField("c", IntegerType, nullable = true)
          )
        )
      )
    )
    expected.write.format("avro").save(s"$workspace/avro/fileWithoutSchema/")
    val actual   = new SourceReader(spark).read(Avro(Seq(s"$workspace/avro/fileWithoutSchema/")))

    assertDatasetEquals(actual, expected)
  }

  test("read jdbc"){
    val expected = spark.createDataset(
      Seq(Row(1, "name1"), Row(2, "name2"), Row(3, "name3"), Row(4, "name4"))
    )(
      RowEncoder(
        StructType(
          Seq(
            StructField("ID", IntegerType, nullable = true),
            StructField("NAME", StringType, nullable = true)
          )
        )
      )
    )
    val actual   = new SourceReader(spark).read(
      Jdbc(url = connUrl1, table = "my_table", credentials = None)
    )
    assertDatasetEquals(actual, expected)
  }

  private def executeStatement(conn: Connection, stmt: String): Unit = {
    val statement = conn.createStatement
    statement.execute(stmt)
    statement.close()
  }
}
