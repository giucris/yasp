package it.yasp.core.spark.reader

import it.yasp.core.spark.testutils.SparkTestSuite
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.io.Path

@DoNotDiscover
class ParquetReaderTest extends AnyFunSuite with SparkTestSuite {

  private val workspace = "yasp-core/src/test/resources/ParquetReaderTest"

  override protected def beforeAll(): Unit =
    cleanWorkspace(workspace)

  override protected def afterAll(): Unit =
    cleanWorkspace(workspace)

  def cleanWorkspace(path: String): Unit =
    Path(path).deleteRecursively()

  test("read") {
    val session  = SparkSession.builder().master("local[*]").getOrCreate()
    val expected = session.createDataset(Seq(Row("a", "b", "c")))(
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
    expected.write.parquet(s"$workspace/parquet1/")
    val actual   = new ParquetReader(session).read(s"$workspace/parquet1/")
    assertDatasetEquals(actual, expected)
  }

}
