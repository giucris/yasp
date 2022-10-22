package it.yasp.core.spark.reader

import it.yasp.core.spark.model.Source._
import it.yasp.core.spark.reader.Reader._
import it.yasp.testkit.SparkTestSuite
import it.yasp.testkit.TestUtils._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.{IntegerType, StringType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Connection
import java.sql.DriverManager.{getConnection, registerDriver}

class SourceReaderTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {
  registerDriver(new org.h2.Driver)

  val reader: SourceReader = new SourceReader(spark)
  val workspace            = "yasp-core/src/test/resources/SourceReaderTest"
  val connUrl1: String     = "jdbc:h2:mem:dbs"
  val conn1: Connection    = getConnection(connUrl1)

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
    cleanFolder(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    cleanFolder(workspace)
    super.afterAll()
  }

  test("read csv") {
    createFile(s"$workspace/csv/file1.csv", Seq("id,field1", "1,x", "2,y"))
    val actual = reader.read(
      Format(
        format = "csv",
        schema = Some("id INT, field1 STRING"),
        options = Map(
          "header" -> "true",
          "sep"    -> ",",
          "path"   -> s"$workspace/csv/file1.csv"
        )
      )
    )
    assertDatasetEquals(actual.getOrElse(fail()), expectedDf)
  }

  test("read json") {
    createFile(
      filePath = s"$workspace/json/json1.json",
      rows = Seq(
        "{\"id\":1,\"field1\":\"x\"}",
        "{\"id\":2,\"field1\":\"y\"}"
      )
    )
    val actual = reader.read(
      Format(
        format = "json",
        schema = Some("id INT, field1 STRING"),
        options = Map("path" -> s"$workspace/json/json1.json")
      )
    )

    assertDatasetEquals(actual.getOrElse(fail()), expectedDf)
  }

  test("read parquet") {
    expectedDf.write.parquet(s"$workspace/parquet1/")
    val actual = reader.read(Format("parquet", options = Map("path" -> s"$workspace/parquet1/")))
    assertDatasetEquals(actual.getOrElse(fail()), expectedDf)
  }

  test("read hiveTable") {
    expectedDf.write.saveAsTable(s"my_table_xxx")
    val actual = reader.read(HiveTable("my_table_xxx"))
    assertDatasetEquals(actual.getOrElse(fail()), expectedDf)
  }

  test("read custom") {
    val actual = reader.read(Custom("it.yasp.core.spark.plugin.MyTestReaderPlugin", None))
    assertDatasetEquals(actual.getOrElse(fail()), spark.emptyDataFrame)
  }
}
