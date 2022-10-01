package it.yasp.core.spark.reader

import it.yasp.core.spark.model.Source.Format
import it.yasp.core.spark.reader.Reader.FormatReader
import it.yasp.testkit.TestUtils.createFile
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{IntegerType => _, LongType => _, StringType => _, _}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Connection
import java.sql.DriverManager.{getConnection, registerDriver}

class FormatReaderTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {
  registerDriver(new org.h2.Driver)

  val reader: FormatReader = new FormatReader(spark)
  val workspace: String    = "yasp-core/src/test/resources/FormatReaderTest"
  val dbConnUrl: String    = "jdbc:h2:mem:dbr"
  val dbConn: Connection   = getConnection(dbConnUrl, "usr", "pwd")

  val expectedDf: Dataset[Row] = spark.createDataset(Seq(Row(1, "x"), Row(2, "y")))(
    RowEncoder(
      StructType(
        Seq(
          StructField("id", IntegerType, nullable = true),
          StructField("field1", StringType, nullable = true)
        )
      )
    )
  )

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.beforeAll()
    executeStatement(
      conn = dbConn,
      stmt = "CREATE TABLE my_table (id INT,field1 VARCHAR(20),PRIMARY KEY (id))"
    )
    executeStatement(
      conn = dbConn,
      stmt = "INSERT INTO my_table VALUES (1, 'x'), (2,'y')"
    )
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  test("read avro") {
    expectedDf.write.format("avro").save(s"$workspace/avro/input1/")
    val actual = reader.read(
      Format(
        format = "avro",
        options = Map("path" -> s"$workspace/avro/input1/")
      )
    )
    assertDatasetEquals(actual, expectedDf)
  }

  test("read csv with header") {
    createFile(s"$workspace/csv/input1/file1.csv", Seq("id,field1", "1,x"))
    createFile(s"$workspace/csv/input1/file2.csv", Seq("id,field1", "2,y"))

    val actual = reader.read(
      Format(
        format = "csv",
        options = Map("header" -> "true", "path" -> s"$workspace/csv/input1/")
      )
    )
    assertDatasetEquals(actual, expectedDf.withColumn("id", col("id").cast(StringType)))
  }

  test("read csv with header and custom sep") {
    createFile(s"$workspace/csv/input2/file1.csv", Seq("id|field1", "1|x"))
    createFile(s"$workspace/csv/input2/file2.csv", Seq("id|field1", "2|y"))

    val actual = reader.read(
      Format(
        "csv",
        None,
        Map("header" -> "true", "sep" -> "|", "path" -> s"$workspace/csv/input2/")
      )
    )
    assertDatasetEquals(actual, expectedDf.withColumn("id", col("id").cast(StringType)))
  }

  test("read csv with header custom sep and schema") {
    createFile(s"$workspace/csv/input3/file1.csv", Seq("id|field1", "1|x"))
    createFile(s"$workspace/csv/input3/file2.csv", Seq("id|field1", "2|y"))

    val actual = reader.read(
      Format(
        "csv",
        Some("id INT, field1 STRING"),
        Map("header" -> "true", "sep" -> "|", "path" -> s"$workspace/csv/input3/")
      )
    )
    assertDatasetEquals(actual, expectedDf)
  }

  test("read json with schema") {
    createFile(
      filePath = s"$workspace/json/input1/file1.json",
      rows = Seq("{\"id\":1,\"field1\":\"x\"}")
    )
    createFile(
      filePath = s"$workspace/json/input1/file2.json",
      rows = Seq("{\"id\":2,\"field1\":\"y\"}")
    )
    val actual = reader.read(
      Format(
        format = "json",
        schema = Some("id INT, field1 STRING"),
        options = Map("path" -> s"$workspace/json/input1/")
      )
    )
    assertDatasetEquals(actual, expectedDf)
  }

  test("read orc") {
    expectedDf.write.orc(s"$workspace/orc/input1/")
    val actual = reader.read(
      Format(format = "orc", options = Map("path" -> s"$workspace/orc/input1/"))
    )
    assertDatasetEquals(actual, expectedDf)
  }

  test("read parquet") {
    expectedDf.write.parquet(s"$workspace/parquet/input1/")
    val actual = reader.read(
      Format(
        format = "parquet",
        options = Map("path" -> s"$workspace/parquet/input1//")
      )
    )
    assertDatasetEquals(actual, expectedDf)
  }

  test("read xml") {
    createFile(
      s"$workspace/xml/input1/file.xml",
      Seq(
        "<root>",
        "<id>1</id>",
        "<field1>x</field1>",
        "</root>",
        "<root>",
        "<id>2</id>",
        "<field1>y</field1>",
        "</root>"
      )
    )
    val actual = reader.read(
      Format(
        format = "xml",
        schema = Some("id INT, field1 STRING"),
        options = Map("path" -> s"$workspace/xml/input1/file.xml", "rowTag" -> "root")
      )
    )
    assertDatasetEquals(actual, expectedDf)
  }

  test("read jdbc table") {
    val expected = expectedDf
      .withMetadata("ID", new MetadataBuilder().putLong("scale", 0).build())
      .withMetadata("FIELD1", new MetadataBuilder().putLong("scale", 0).build())

    val actual = reader.read(
      Format(
        format = "jdbc",
        options = Map(
          "url"      -> dbConnUrl,
          "user"     -> "usr",
          "password" -> "pwd",
          "dbTable"  -> "my_table"
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
