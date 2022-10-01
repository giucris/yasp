package it.yasp.core.spark.reader

import it.yasp.core.spark.model.Source.Format
import it.yasp.core.spark.reader.Reader.FormatReader
import it.yasp.testkit.TestUtils.createFile
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{IntegerType => _, LongType => _, StringType => _, _}
import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Connection
import java.sql.DriverManager.{getConnection, registerDriver}

class FormatReaderTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {

  val reader: FormatReader = new FormatReader(spark)

  private val workspace        = "yasp-core/src/test/resources/FormatReaderTest"
  private val connUrl1: String = "jdbc:h2:mem:db1"
  private val connUrl2: String = "jdbc:h2:mem:db2"

  registerDriver(new org.h2.Driver)
  private val conn1: Connection = getConnection(connUrl1)
  private val conn2: Connection = getConnection(connUrl2, "usr", "pwd")

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.beforeAll()
    executeStatement(
      conn = conn1,
      stmt = "CREATE TABLE my_table (id INT,name VARCHAR(20),PRIMARY KEY (id))"
    )
    executeStatement(
      conn = conn2,
      stmt = "CREATE TABLE my_table (id INT,name VARCHAR(20),PRIMARY KEY (id))"
    )
    executeStatement(
      conn = conn1,
      stmt = "INSERT INTO my_table VALUES (1, 'name1'), (2,'name2'), (3,'name3'),(4,'name4')"
    )
    executeStatement(
      conn = conn2,
      stmt = "INSERT INTO my_table VALUES (1, 'name1'), (2,'name2'), (3,'name3'),(4,'name4')"
    )
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
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
    val actual   =
      reader.read(Format("avro", options = Map("path" -> s"$workspace/avro/fileWithoutSchema/")))
    assertDatasetEquals(actual, expected)
  }

  test("read csv") {
    createFile(s"$workspace/csv/input1/file1.csv", Seq("h1,h2,h3", "a,b,c"))
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
    val actual   = reader.read(Format("csv", options = Map("path" -> s"$workspace/csv/input1/")))
    assertDatasetEquals(actual, expected)
  }

  test("read csv with header") {
    createFile(s"$workspace/csv/input2/file1.csv", Seq("h1,h2,h3", "a,b,c"))
    createFile(s"$workspace/csv/input2/file2.csv", Seq("h1,h2,h3", "d,e,f"))
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
      Format("csv", options = Map("header" -> "true", "path" -> s"$workspace/csv/input2/"))
    )
    assertDatasetEquals(actual, expected)
  }

  test("read csv with header and custom sep") {
    createFile(s"$workspace/csv/input3/file1.csv", Seq("h1|h2|h3", "a|b|c"))
    createFile(s"$workspace/csv/input3/file2.csv", Seq("h1|h2|h3", "d|e|f"))
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
      Format(
        "csv",
        None,
        Map("header" -> "true", "sep" -> "|", "path" -> s"$workspace/csv/input3/")
      )
    )
    assertDatasetEquals(actual, expected)
  }

  test("read csv with header custom sep and schema") {
    createFile(s"$workspace/csv/input4/file1.csv", Seq("h1|h2|h3", "1|b|c"))
    createFile(s"$workspace/csv/input4/file2.csv", Seq("h1|h2|h3", "2|e|f"))
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

    val actual = reader.read(
      Format(
        "csv",
        Some("h1 INT, h2 STRING, h3 STRING"),
        Map("header" -> "true", "sep" -> "|", "path" -> s"$workspace/csv/input4/")
      )
    )
    assertDatasetEquals(actual, expected)
  }

  test("read csv with header custom sep schema and corrupt record") {
    createFile(s"$workspace/csv/input5/file1.csv", Seq("h1|h2|h3", "1|b|c"))
    createFile(s"$workspace/csv/input5/file2.csv", Seq("h1|h2|h3", "x|x|x"))

    val expected = spark.createDataset(Seq(Row(1, "b", "c", null), Row(null, "x", "x", "x|x|x")))(
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

    val actual = reader.read(
      Format(
        format = "csv",
        schema = Some("h1 INT, h2 STRING, h3 STRING,_corrupt_record STRING"),
        options = Map("header" -> "true", "sep" -> "|", "path" -> s"$workspace/csv/input5/")
      )
    )

    assertDatasetEquals(actual, expected)
  }

  test("read json") {
    createFile(
      filePath = s"$workspace/json/input1/file1.json",
      rows = Seq(
        "{\"a\":\"file1\",\"b\":1}",
        "{\"a\":\"file1\",\"c\":2}"
      )
    )
    createFile(
      filePath = s"$workspace/json/input1/file2.json",
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
    val actual   = reader.read(Format("json", options = Map("path" -> s"$workspace/json/input1/")))

    assertDatasetEquals(actual, expected)
  }

  test("read json with schema") {
    createFile(
      filePath = s"$workspace/json/input2/file1.json",
      rows = Seq(
        "{\"a\":\"file1\",\"b\":1}"
      )
    )
    createFile(
      filePath = s"$workspace/json/input2/file2.json",
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
      Format(
        "json",
        schema = Some("a STRING, b LONG, c LONG, d STRING"),
        options = Map("path" -> s"$workspace/json/input2/")
      )
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

    expected.write.orc(s"$workspace/orc/input1/")

    val actual = reader.read(Format("orc", options = Map("path" -> s"$workspace/orc/input1/")))
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
      .parquet(s"$workspace/parquet/input1/")

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

    val actual =
      reader.read(Format("parquet", options = Map("path" -> s"$workspace/parquet/input1//")))
    assertDatasetEquals(actual, expected)
  }

  test("read parquet with mergeSchema") {
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
      .parquet(s"$workspace/parquet/input2/")

    spark
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
      .write
      .mode(SaveMode.Append)
      .parquet(s"$workspace/parquet/input2/")

    val expected = spark
      .createDataset(Seq(Row("a", "b", "c", null), Row("d", "e", "f", "g")))(
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
    val actual   = reader.read(
      Format(
        "parquet",
        options = Map("path" -> s"$workspace/parquet/input2/", "mergeSchema" -> "true")
      )
    )
    assertDatasetEquals(actual, expected)
  }

  test("read xml") {
    TestUtils.createFile(
      s"$workspace/xml/input1/file.xml",
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

    val actual = reader.read(
      Format("xml", options = Map("path" -> s"$workspace/xml/input1/file.xml", "rowTag" -> "root"))
    )
    assertDatasetEquals(actual, expected)
  }

  test("read jdbc table") {
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

    val actual =
      reader.read(Format("jdbc", options = Map("url" -> connUrl1, "dbTable" -> "my_table")))

    assertDatasetEquals(actual, expected)
  }

  test("read jdbc table with credentials") {
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
      Format(
        "jdbc",
        options = Map(
          "url"      -> connUrl2,
          "user"     -> "usr",
          "password" -> "pwd",
          "dbTable"  -> "my_table"
        )
      )
    )
    assertDatasetEquals(actual, expected)
  }

  test("read jdbc table query with credentials") {
    val expected = spark
      .createDataset(Seq(Row(1)))(
        RowEncoder(
          StructType(
            Seq(
              StructField("ID", IntegerType, nullable = true)
            )
          )
        )
      )
      .withMetadata("ID", new MetadataBuilder().putLong("scale", 0).build())

    val actual = reader.read(
      Format(
        "jdbc",
        options = Map(
          "url"      -> connUrl2,
          "user"     -> "usr",
          "password" -> "pwd",
          "dbTable"  -> "(select ID from my_table where id=1) test"
        )
      )
    )

    assertDatasetEquals(actual, expected)
  }

  private def executeStatement(conn: Connection, stmt: String): Unit = {
    val statement = conn.createStatement
    statement.execute(stmt)
    statement.close()
  }

}
