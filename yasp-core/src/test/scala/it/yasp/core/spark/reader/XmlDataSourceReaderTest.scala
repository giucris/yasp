package it.yasp.core.spark.reader

import com.databricks.spark.xml._
import it.yasp.core.spark.model.DataSource.Xml
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class XmlDataSourceReaderTest extends AnyFunSuite with SparkTestSuite {

  private val workspace = "yasp-core/src/test/resources/XmlReaderTest"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    TestUtils.cleanFolder(workspace)
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  test("read a single xml file") {
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

    val actual = new XmlDataSourceReader(spark).read(Xml(Seq(s"$workspace/xml/file.xml"), "root"))
    assertDatasetEquals(actual, expected)
  }

  test("read multiple xml file") {
    TestUtils.createFile(
      s"$workspace/xmls/file1.xml",
      Seq(
        "<root>",
        "<field1>value1</field1>",
        "<field2>value2</field2>",
        "</root>"
      )
    )
    TestUtils.createFile(
      s"$workspace/xmls/file2.xml",
      Seq(
        "<root>",
        "<field1>value1</field1>",
        "<field3>value3</field3>",
        "</root>"
      )
    )

    val expected = spark.createDataset(
      Seq(Row("value1", "value2", null), Row("value1", null, "value3"))
    )(
      RowEncoder(
        StructType(
          Seq(
            StructField("field1", StringType),
            StructField("field2", StringType),
            StructField("field3", StringType)
          )
        )
      )
    )

    val actual = new XmlDataSourceReader(spark).read(
      Xml(Seq(s"$workspace/xmls/file1.xml", s"$workspace/xmls/file2.xml"), "root")
    )
    assertDatasetEquals(actual, expected)
  }

  /*
    test("read multiple xml file path"){
      TestUtil.createFile(Seq(
        "<root>",
        "<field1>value1</field1>",
        "<field2>value2</field2>",
        "</root>"
      ), s"$workspaceFolder/xml1/file.xml")

      TestUtil.createFile(Seq(
        "<root>",
        "<field1>value1</field1>",
        "<field2>value2</field2>",
        "</root>"
      ), s"$workspaceFolder/xml2/file.xml")

      val expected = spark.createDataset(Seq(
        Row("value1","value2",null,Paths.get(s"$workspaceFolder/xml1/file.xml").toUri.toURL.toString),
        Row("value1","value2",null,Paths.get(s"$workspaceFolder/xml2/file.xml").toUri.toURL.toString)
      ))(RowEncoder(StructType(Seq(
        StructField("field1", StringType),
        StructField("field2", StringType),
        StructField("_corrupt_record", StringType),
        StructField("_input_file_name", StringType, false)
      ))))

      val actual = DataReader.xml(spark,Some(Map("rowTag"->"root"))).read(Seq(s"$workspaceFolder/xml1/",s"$workspaceFolder/xml2/"))

      assertDatasetEquals(actual, expected)
    }

    test("read handling corrupt record"){
      TestUtil.createFile(Seq(
        "<root>",
        "<field1>value1</field1>",
        "<field2>value2</field2>",
        "</root>"
      ), s"$workspaceFolder/xml1/file.xml")

      TestUtil.createFile(Seq(
        "<root><field1>value1</root>"
      ), s"$workspaceFolder/xml2/file.xml")

      val expected = spark.createDataset(Seq(
        Row("value1","value2",null,Paths.get(s"$workspaceFolder/xml1/file.xml").toUri.toURL.toString),
        Row(null,null,"<root><field1>value1</root>",Paths.get(s"$workspaceFolder/xml2/file.xml").toUri.toURL.toString)
      ))(RowEncoder(StructType(Seq(
        StructField("field1", StringType),
        StructField("field2", StringType),
        StructField("_corrupt_record", StringType),
        StructField("_input_file_name", StringType, false)
      ))))
      val actual = DataReader.xml(spark,Some(Map("rowTag"->"root"))).read(Seq(s"$workspaceFolder/xml1/",s"$workspaceFolder/xml2/"))
      assertDatasetEquals(actual, expected)
    }
   */
}

class XmlDataSourceReader(spark: SparkSession) extends DataSourceReader[Xml] {
  override def read(source: Xml): Dataset[Row] =
    spark.read.option("rowTag", source.rowTag).xml(source.paths.mkString(","))
}
