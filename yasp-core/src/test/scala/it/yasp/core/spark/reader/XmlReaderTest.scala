package it.yasp.core.spark.reader

import it.yasp.core.spark.model.Source.Xml
import it.yasp.core.spark.reader.Reader.XmlReader
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class XmlReaderTest extends AnyFunSuite with SparkTestSuite {

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

    val actual = new XmlReader(spark).read(Xml(s"$workspace/xml/file.xml", Some(Map("rowTag"->"root"))))
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

    val actual = new XmlReader(spark).read(
      Xml(s"$workspace/xmls/file1.xml,$workspace/xmls/file2.xml", Some(Map("rowTag"->"root")))
    )
    assertDatasetEquals(actual, expected)
  }

}
