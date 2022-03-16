package it.yasp.core.spark.writer

import it.yasp.testkit.{SharedSparkSession, TestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover}

@DoNotDiscover
class ParquetWriterTest extends AnyFunSuite with SharedSparkSession with BeforeAndAfterAll {

  private val workspace = "yasp-core/src/test/resources/ParquetWriterTest"

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  test("write") {
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
      .parquet(s"$workspace/parquet1/")
  }
}
