package it.yasp.core.spark.reader

import it.yasp.core.spark.model.Source._
import it.yasp.core.spark.reader.Reader._
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.LongType
import org.apache.spark.sql.types._
import org.h2.Driver
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Connection
import java.sql.DriverManager.{getConnection, registerDriver}

class SourceReaderTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {

  private val workspace        = "yasp-core/src/test/resources/SourceReaderTest"
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

  val reader = new SourceReader(spark)

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
    val actual   = reader.read(
      Csv(
        csv = s"$workspace/singleCsv/file1.csv",
        options = Map(
          "header" -> "false",
          "sep"    -> ","
        )
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

    val actual = reader.read(Json(s"$workspace/json/json1.json"))

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
    val actual   = reader.read(Parquet(s"$workspace/parquet1/"))
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

    val actual =
      reader.read(Xml(s"$workspace/xml/file.xml", Map("rowTag" -> "root")))
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
    val actual   = reader.read(Avro(s"$workspace/avro/fileWithoutSchema/"))

    assertDatasetEquals(actual, expected)
  }

  test("read jdbc") {
    val expected = spark
      .createDataset(
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
      .withMetadata("ID", new MetadataBuilder().putLong("scale", 0).build())
      .withMetadata("NAME", new MetadataBuilder().putLong("scale", 0).build())

    val actual = reader.read(
      Jdbc(jdbcUrl = connUrl1, jdbcAuth = None, Map("dbTable" -> "my_table"))
    )
    assertDatasetEquals(actual, expected)
  }

  test("read orc") {
    val expected = spark
      .createDataset(Seq(Row("d", "e", "f", "g")))(
        RowEncoder(
          StructType(
            Seq(
              StructField("h0", StringType, nullable = true),
              StructField("h1", StringType, nullable = true),
              StructField("h2", StringType, nullable = true),
              StructField("h3", StringType, nullable = true)
            )
          )
        )
      )

    expected.write.orc(s"$workspace/orc/")

    val actual = reader.read(Orc(s"$workspace/orc/"))
    assertDatasetEquals(actual, expected)
  }

  private def executeStatement(conn: Connection, stmt: String): Unit = {
    val statement = conn.createStatement
    statement.execute(stmt)
    statement.close()
  }
}
