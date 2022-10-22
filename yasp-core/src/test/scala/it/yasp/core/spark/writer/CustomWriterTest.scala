package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest.Custom
import it.yasp.core.spark.plugin.PluginProvider
import it.yasp.core.spark.writer.Writer.CustomWriter
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.{IntegerType, StringType}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class CustomWriterTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {
  val workspace: String = "yasp-core/src/test/resources/CustomWriterTest"

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
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  test("read with MyTestWriterPlugin") {
    new CustomWriter(new PluginProvider).write(
      expectedDf,
      Custom("it.yasp.core.spark.plugin.MyTestWriterPlugin", Some(Map("path" -> s"$workspace/custom")))
    )
    val actual = spark.read.parquet(s"$workspace/custom")
    assertDatasetEquals(actual, expectedDf)
  }

}
