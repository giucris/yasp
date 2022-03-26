package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest.Parquet
import it.yasp.core.spark.writer.Writer.DestWriter
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover}

@DoNotDiscover
class DestWriterTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {
  private val workspace = "yasp-core/src/test/resources/DestWriterTest"

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  test("write dest parquet") {
    val expected = spark.createDataset(Seq(Row("a", "b", "c")))(
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
    new DestWriter().write(expected, Parquet(s"$workspace/parquet1/"))
    val actual   = spark.read.parquet(s"$workspace/parquet1/")

    assertDatasetEquals(actual, expected)
  }

}
