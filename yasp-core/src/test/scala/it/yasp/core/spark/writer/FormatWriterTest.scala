package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest.Format
import it.yasp.core.spark.writer.Writer.FormatWriter
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Connection
import java.sql.DriverManager.{getConnection, registerDriver}
import java.util.Properties

class FormatWriterTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {
  registerDriver(new org.h2.Driver)

  val writer: FormatWriter = new FormatWriter()
  val workspace            = "yasp-core/src/test/resources/CsvWriterTest"
  val dbConnUrl: String    = "jdbc:h2:mem:dbw1"
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
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  test("write csv") {
    writer.write(
      expectedDf,
      Format("csv", options = Map("header" -> "true", "path" -> s"$workspace/csv/output1/"))
    )

    val actual = spark.read
      .option("header", "true")
      .schema("id INT, field1 STRING")
      .csv(s"$workspace/csv/output1/")

    assertDatasetEquals(actual, expectedDf)
  }

  test("write csv with partitionBy") {
    writer.write(
      expectedDf,
      Format(
        "csv",
        options = Map("header" -> "true", "path" -> s"$workspace/csv/output2/"),
        partitionBy = Seq("id")
      )
    )

    val actual = spark.read
      .options(Map("basePath" -> s"$workspace/csv/output2/", "header" -> "true"))
      .csv(s"$workspace/csv/output2/id=1/", s"$workspace/csv/output2/id=2/")
      .select("id", "field1")

    assertDatasetEquals(actual, expectedDf)
  }

  test("write csv with append mode") {
    writer.write(
      expectedDf,
      Format(
        "csv",
        options = Map("header" -> "true", "path" -> s"$workspace/csv/output3/"),
        mode = Some("append")
      )
    )
    writer.write(
      expectedDf,
      Format(
        "csv",
        options = Map("header" -> "true", "path" -> s"$workspace/csv/output3/"),
        mode = Some("append")
      )
    )

    val actual = spark.read
      .option("header", "true")
      .schema("id INT, field1 STRING")
      .csv(s"$workspace/csv/output3/")

    assertDatasetEquals(actual, expectedDf.union(expectedDf))
  }

  test("write csv with overwrite mode") {
    writer.write(
      expectedDf,
      Format(
        "csv",
        options = Map("header" -> "true", "path" -> s"$workspace/csv/output4/"),
        mode = Some("overwrite")
      )
    )
    writer.write(
      expectedDf,
      Format(
        "csv",
        options = Map("header" -> "true", "path" -> s"$workspace/csv/output4/"),
        mode = Some("overwrite")
      )
    )
    val actual = spark.read
      .option("header", "true")
      .schema("id INT, field1 STRING")
      .csv(s"$workspace/csv/output4/")
    assertDatasetEquals(actual, expectedDf)
  }

  test("write json") {
    writer.write(
      expectedDf,
      Format("json", options = Map("path" -> s"$workspace/json/output1/"))
    )

    val actual = spark.read
      .schema("id INT, field1 STRING")
      .json(s"$workspace/json/output1/")

    assertDatasetEquals(actual, expectedDf)
  }

  test("write json with partitionBy") {
    writer.write(
      expectedDf,
      Format(
        "json",
        options = Map("path" -> s"$workspace/json/output2/"),
        partitionBy = Seq("id")
      )
    )

    val actual = spark.read
      .options(Map("basePath" -> s"$workspace/json/output2/"))
      .json(s"$workspace/json/output2/id=1/", s"$workspace/json/output2/id=2/")
      .select("id", "field1")

    assertDatasetEquals(actual, expectedDf)
  }

  test("write json append mode") {
    writer.write(
      expectedDf,
      Format(
        "json",
        options = Map("path" -> s"$workspace/json/output3/"),
        mode = Some("append")
      )
    )
    writer.write(
      expectedDf,
      Format(
        "json",
        options = Map("path" -> s"$workspace/json/output3/"),
        mode = Some("append")
      )
    )

    val actual = spark.read
      .option("header", "true")
      .schema("id INT, field1 STRING")
      .json(s"$workspace/json/output3/")

    assertDatasetEquals(actual, expectedDf.union(expectedDf))
  }

  test("write json with overwrite mode") {
    writer.write(
      expectedDf,
      Format(
        "json",
        options = Map("path" -> s"$workspace/json/output4/"),
        mode = Some("overwrite")
      )
    )
    writer.write(
      expectedDf,
      Format(
        "json",
        options = Map("path" -> s"$workspace/json/output4/"),
        mode = Some("overwrite")
      )
    )
    val actual = spark.read
      .option("header", "true")
      .schema("id INT, field1 STRING")
      .json(s"$workspace/json/output4/")

    assertDatasetEquals(actual, expectedDf)
  }

  test("write parquet") {
    writer.write(
      expectedDf,
      Format(
        format = "parquet",
        options = Map("path" -> s"$workspace/parquet/input1/")
      )
    )
    val actual = spark.read.parquet(s"$workspace/parquet/input1/")
    assertDatasetEquals(actual, expectedDf)
  }

  test("write parquet with partitionBy") {
    writer.write(
      expectedDf,
      Format(
        format = "parquet",
        options = Map("path" -> s"$workspace/parquet/input2/"),
        partitionBy = Seq("id")
      )
    )

    val actual = spark.read
      .option("basePath", s"$workspace/parquet/input2/")
      .parquet(s"$workspace/parquet/input2/id=1/", s"$workspace/parquet/input2/id=2/")
      .select("id", "field1")

    assertDatasetEquals(actual, expectedDf)
  }

  test("write parquet with append mode") {
    writer.write(
      expectedDf,
      Format(
        format = "parquet",
        options = Map("path" -> s"$workspace/parquet/input3/"),
        mode = Some("append")
      )
    )
    writer.write(
      expectedDf,
      Format(
        format = "parquet",
        options = Map("path" -> s"$workspace/parquet/input3/"),
        mode = Some("append")
      )
    )

    val actual = spark.read.parquet(s"$workspace/parquet/input3/")
    assertDatasetEquals(actual, expectedDf.union(expectedDf))
  }

  test("write parquet with overwrite mode") {
    writer.write(
      expectedDf,
      Format(
        format = "parquet",
        options = Map("path" -> s"$workspace/parquet/input4/"),
        mode = Some("overwrite")
      )
    )
    writer.write(
      expectedDf,
      Format(
        format = "parquet",
        options = Map("path" -> s"$workspace/parquet/input4/"),
        mode = Some("overwrite")
      )
    )

    val actual = spark.read.parquet(s"$workspace/parquet/input4/")
    assertDatasetEquals(actual, expectedDf)
  }

  test("write jdbc table") {
    writer.write(
      expectedDf,
      Format(
        "jdbc",
        options = Map(
          "url"      -> dbConnUrl,
          "user"     -> "usr",
          "password" -> "pwd",
          "dbTable"  -> "my_test_table"
        )
      )
    )
    val prop = new Properties()
    prop.setProperty("user", "usr")
    prop.setProperty("password", "pwd")

    val actual = spark.read.jdbc(dbConnUrl, "my_test_table", prop)

    val expected = expectedDf
      .withMetadata("id", new MetadataBuilder().putLong("scale", 0).build())
      .withMetadata("field1", new MetadataBuilder().putLong("scale", 0).build())

    assertDatasetEquals(actual, expected)
  }
}
