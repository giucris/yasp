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
          StructField("ID", IntegerType, nullable = true),
          StructField("FIELD1", StringType, nullable = true)
        )
      )
    )
  )

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    spark.conf.set(
      "spark.sql.catalog.spark_catalog",
      "org.apache.iceberg.spark.SparkSessionCatalog"
    )
    spark.conf.set("spark.sql.catalog.spark_catalog.type", "hadoop")
    spark.conf.set("spark.sql.catalog.spark_catalog.warehouse", s"$workspace/iceberg_spark_catalog")
    spark.conf.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.local.type", "hadoop")
    spark.conf.set("spark.sql.catalog.local.warehouse", s"$workspace/iceberg_local_catalog")

    super.beforeAll()
    executeStatement(
      conn = dbConn,
      stmt = "CREATE TABLE my_table (ID INT,FIELD1 VARCHAR(20),PRIMARY KEY (ID))"
    )
    executeStatement(
      conn = dbConn,
      stmt = "INSERT INTO my_table VALUES (1, 'x'), (2,'y')"
    )
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    spark.conf.unset("spark.sql.catalog.spark_catalog")
    spark.conf.unset("spark.sql.catalog.spark_catalog.type")
    spark.conf.unset("spark.sql.catalog.spark_catalog.warehouse")
    spark.conf.unset("spark.sql.catalog.local")
    spark.conf.unset("spark.sql.catalog.local.type")
    spark.conf.unset("spark.sql.catalog.local.warehouse")
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
    assertDatasetEquals(actual.getOrElse(fail()), expectedDf)
  }

  test("read csv with header") {
    createFile(s"$workspace/csv/input1/file1.csv", Seq("ID,FIELD1", "1,x"))
    createFile(s"$workspace/csv/input1/file2.csv", Seq("ID,FIELD1", "2,y"))

    val actual = reader.read(
      Format(
        format = "csv",
        options = Map("header" -> "true", "path" -> s"$workspace/csv/input1/")
      )
    )
    assertDatasetEquals(
      actual.getOrElse(fail()),
      expectedDf.withColumn("ID", col("ID").cast(StringType))
    )
  }

  test("read csv with header and custom sep") {
    createFile(s"$workspace/csv/input2/file1.csv", Seq("ID|FIELD1", "1|x"))
    createFile(s"$workspace/csv/input2/file2.csv", Seq("ID|FIELD1", "2|y"))

    val actual = reader.read(
      Format(
        "csv",
        None,
        Map("header" -> "true", "sep" -> "|", "path" -> s"$workspace/csv/input2/")
      )
    )
    assertDatasetEquals(
      actual.getOrElse(fail()),
      expectedDf.withColumn("ID", col("ID").cast(StringType))
    )
  }

  test("read csv with header custom sep and schema") {
    createFile(s"$workspace/csv/input3/file1.csv", Seq("ID|FIELD1", "1|x"))
    createFile(s"$workspace/csv/input3/file2.csv", Seq("ID|FIELD1", "2|y"))

    val actual = reader.read(
      Format(
        "csv",
        Some("ID INT, FIELD1 STRING"),
        Map("header" -> "true", "sep" -> "|", "path" -> s"$workspace/csv/input3/")
      )
    )
    assertDatasetEquals(actual.getOrElse(fail()), expectedDf)
  }

  test("read json with schema") {
    createFile(
      filePath = s"$workspace/json/input1/file1.json",
      rows = Seq("{\"ID\":1,\"FIELD1\":\"x\"}")
    )
    createFile(
      filePath = s"$workspace/json/input1/file2.json",
      rows = Seq("{\"ID\":2,\"FIELD1\":\"y\"}")
    )
    val actual = reader.read(
      Format(
        format = "json",
        schema = Some("ID INT, FIELD1 STRING"),
        options = Map("path" -> s"$workspace/json/input1/")
      )
    )
    assertDatasetEquals(actual.getOrElse(fail()), expectedDf)
  }

  test("read orc") {
    expectedDf.write.orc(s"$workspace/orc/input1/")
    val actual = reader.read(
      Format(format = "orc", options = Map("path" -> s"$workspace/orc/input1/"))
    )
    assertDatasetEquals(actual.getOrElse(fail()), expectedDf)
  }

  test("read parquet") {
    expectedDf.write.parquet(s"$workspace/parquet/input1/")
    val actual = reader.read(
      Format(
        format = "parquet",
        options = Map("path" -> s"$workspace/parquet/input1//")
      )
    )
    assertDatasetEquals(actual.getOrElse(fail()), expectedDf)
  }

  test("read xml") {
    createFile(
      s"$workspace/xml/input1/file.xml",
      Seq(
        "<root>",
        "<ID>1</ID>",
        "<FIELD1>x</FIELD1>",
        "</root>",
        "<root>",
        "<ID>2</ID>",
        "<FIELD1>y</FIELD1>",
        "</root>"
      )
    )
    val actual = reader.read(
      Format(
        format = "xml",
        schema = Some("ID INT, FIELD1 STRING"),
        options = Map("path" -> s"$workspace/xml/input1/file.xml", "rowTag" -> "root")
      )
    )
    assertDatasetEquals(actual.getOrElse(fail()), expectedDf)
  }

  test("read jdbc table") {
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
    assertDatasetEquals(actual.getOrElse(fail()), expectedDf)
  }

  test("read delta table") {
    expectedDf.write.format("delta").save(s"$workspace/deltaTable1")
    val actual = reader.read(Format("delta", options = Map("path" -> s"$workspace/deltaTable1")))
    assertDatasetEquals(actual.getOrElse(fail()), expectedDf)
  }

  test("read delta table partitioned ") {
    expectedDf.write.format("delta").partitionBy("ID").save(s"$workspace/deltaTable2")
    val actual = reader.read(Format("delta", options = Map("path" -> s"$workspace/deltaTable2")))
    assertDatasetEquals(actual.getOrElse(fail()), expectedDf)
  }

  test("read iceberg table") {
    expectedDf.writeTo("local.db.my_table").create()
    val actual = reader.read(Format("iceberg", options = Map("path" -> s"local.db.my_table")))
    assertDatasetEquals(actual.getOrElse(fail()), expectedDf)
  }

  private def executeStatement(conn: Connection, stmt: String): Unit = {
    val statement = conn.createStatement
    statement.execute(stmt)
    statement.close()
  }

}
