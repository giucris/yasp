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

class FormatCsvWriterTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {
  val writer: FormatWriter = new FormatWriter()

  private val workspace = "yasp-core/src/test/resources/CsvWriterTest"

  private val connUrl1: String = "jdbc:h2:mem:db3"
  private val connUrl2: String = "jdbc:h2:mem:db4"

  registerDriver(new org.h2.Driver)
  val conn1: Connection = getConnection(connUrl1)
  val conn2: Connection = getConnection(connUrl2, "usr", "pwd")

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  val df: Dataset[Row] = spark.createDataset(Seq(Row("a", "b", "c"), Row("x", "y", "z")))(
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

  private val df2: Dataset[Row] = spark
    .createDataset(
      Seq(
        Row(1, "name1"),
        Row(2, "name2"),
        Row(3, "name3"),
        Row(4, "name4")
      )
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

  test("write csv") {
    writer.write(df, Format("csv", options = Map("path" -> s"$workspace/csv/output1/")))

    val actual   = spark.read.csv(s"$workspace/csv/output1/")
    val expected = spark.createDataset(Seq(Row("a", "b", "c"), Row("x", "y", "z")))(
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
    assertDatasetEquals(actual, expected)
  }

  test("write csv with partitionBy") {
    writer.write(
      df,
      Format(
        "csv",
        options = Map("header" -> "true", "path" -> s"$workspace/csv/output2/"),
        partitionBy = Seq("h1")
      )
    )

    val actual = spark.read
      .options(Map("basePath" -> s"$workspace/csv/output2/", "header" -> "true"))
      .csv(s"$workspace/csv/output2/h1=a/", s"$workspace/csv/output2/h1=x/")

    val expected = spark.createDataset(Seq(Row("b", "c", "a"), Row("y", "z", "x")))(
      RowEncoder(
        StructType(
          Seq(
            StructField("h2", StringType, nullable = true),
            StructField("h3", StringType, nullable = true),
            StructField("h1", StringType, nullable = true)
          )
        )
      )
    )
    assertDatasetEquals(actual, expected)
  }

  test("write csv with append mode") {
    writer.write(
      df,
      Format(
        "csv",
        options = Map("header" -> "true", "path" -> s"$workspace/csv/output3/"),
        mode = Some("append")
      )
    )
    writer.write(
      df,
      Format(
        "csv",
        options = Map("header" -> "true", "path" -> s"$workspace/csv/output3/"),
        mode = Some("append")
      )
    )

    val actual   = spark.read.option("header", "true").csv(s"$workspace/csv/output3/")
    val expected = df.union(df)
    assertDatasetEquals(actual, expected)
  }

  test("write csv with overwrite mode") {
    writer.write(
      df,
      Format(
        "csv",
        options = Map("header" -> "true", "path" -> s"$workspace/csv/output4/"),
        mode = Some("overwrite")
      )
    )
    writer.write(
      df,
      Format(
        "csv",
        options = Map("header" -> "true", "path" -> s"$workspace/csv/output4/"),
        mode = Some("overwrite")
      )
    )
    val actual = spark.read.option("header", "true").csv(s"$workspace/csv/output4/")
    assertDatasetEquals(actual, df)
  }

  test("write json") {
    writer.write(df, Format("json", options = Map("path" -> s"$workspace/json/output1/")))
    val actual   = spark.read.json(s"$workspace/json/output1/")
    val expected = spark.createDataset(Seq(Row("a", "b", "c"), Row("x", "y", "z")))(
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
    assertDatasetEquals(actual, expected)
  }

  test("write json with partitionBy") {
    writer.write(
      df,
      Format("json", options = Map("path" -> s"$workspace/json/output2/"), partitionBy = Seq("h1"))
    )

    val actual = spark.read
      .options(Map("basePath" -> s"$workspace/json/output2/"))
      .json(s"$workspace/json/output2/h1=a/", s"$workspace/json/output2/h1=x/")

    val expected = spark.createDataset(Seq(Row("b", "c", "a"), Row("y", "z", "x")))(
      RowEncoder(
        StructType(
          Seq(
            StructField("h2", StringType, nullable = true),
            StructField("h3", StringType, nullable = true),
            StructField("h1", StringType, nullable = true)
          )
        )
      )
    )
    assertDatasetEquals(actual, expected)
  }

  test("write json with append mode") {
    writer.write(
      df,
      Format("json", options = Map("path" -> s"$workspace/json/output3/"), mode = Some("append"))
    )
    writer.write(
      df,
      Format("json", options = Map("path" -> s"$workspace/json/output3/"), mode = Some("append"))
    )
    val actual   = spark.read.json(s"$workspace/json/output3/")
    val expected = df.union(df)
    assertDatasetEquals(actual, expected)
  }

  test("write json with overwrite mode") {
    writer.write(
      df,
      Format("json", options = Map("path" -> s"$workspace/json/output4/"), mode = Some("overwrite"))
    )
    writer.write(
      df,
      Format("json", options = Map("path" -> s"$workspace/json/output4/"), mode = Some("overwrite"))
    )

    val actual = spark.read.json(s"$workspace/json/output4/")
    assertDatasetEquals(actual, df)
  }

  test("write parquet") {
    writer.write(df, Format("parquet", options = Map("path" -> s"$workspace/parquet/input1/")))
    val actual = spark.read.parquet(s"$workspace/parquet/input1/")
    assertDatasetEquals(actual, df)
  }

  test("write parquet with partitionBy") {
    writer.write(
      df,
      Format(
        "parquet",
        options = Map("path" -> s"$workspace/parquet/input2/"),
        partitionBy = Seq("h1", "h2")
      )
    )

    val actual = spark.read
      .option("basePath", s"$workspace/parquet/input2/")
      .parquet(s"$workspace/parquet/input2/h1=a/h2=b/")

    val expected = spark.createDataset(Seq(Row("c", "a", "b")))(
      RowEncoder(
        StructType(
          Seq(
            StructField("h3", StringType, nullable = true),
            StructField("h1", StringType, nullable = true),
            StructField("h2", StringType, nullable = true)
          )
        )
      )
    )

    assertDatasetEquals(actual, expected)
  }

  test("write parquet with append mode") {
    writer.write(
      df,
      Format(
        "parquet",
        options = Map("path" -> s"$workspace/parquet/input3/"),
        mode = Some("append")
      )
    )
    writer.write(
      df,
      Format(
        "parquet",
        options = Map("path" -> s"$workspace/parquet/input3/"),
        mode = Some("append")
      )
    )

    val actual   = spark.read.parquet(s"$workspace/parquet/input3/")
    val expected = df.union(df)
    assertDatasetEquals(actual, expected)
  }

  test("write parquet with overwrite mode") {
    writer.write(
      df,
      Format(
        "parquet",
        options = Map("path" -> s"$workspace/parquet/input4/"),
        mode = Some("overwrite")
      )
    )
    writer.write(
      df,
      Format(
        "parquet",
        options = Map("path" -> s"$workspace/parquet/input4/"),
        mode = Some("overwrite")
      )
    )

    val actual = spark.read.parquet(s"$workspace/parquet/input4/")
    assertDatasetEquals(actual, df)
  }

  test("write jdbc table") {
    writer.write(
      df2,
      Format("jdbc", options = Map("url" -> connUrl1, "dbTable" -> "my_test_table"), None)
    )
    val actual = spark.read.jdbc(connUrl1, "my_test_table", new Properties())
    assertDatasetEquals(actual, df2)
  }

  test("write jdbc table with basic credentials") {
    writer.write(
      df2,
      Format(
        "jdbc",
        options = Map(
          "url"      -> connUrl2,
          "user"     -> "usr",
          "password" -> "pwd",
          "dbTable"  -> "my_test_table"
        ),
        None
      )
    )
    val properties = new Properties()
    properties.setProperty("user", "usr")
    properties.setProperty("password", "pwd")
    val actual     = spark.read.jdbc(connUrl2, "my_test_table", properties)
    assertDatasetEquals(actual, df2)
  }

}
