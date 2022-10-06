package it.yasp.core.spark.writer

import it.yasp.core.spark.err.YaspCoreError.WriteError
import it.yasp.core.spark.model.Dest.HiveTable
import it.yasp.core.spark.writer.Writer.HiveTableWriter
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class HiveTableWriterTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {

  val hiveTableWriter: HiveTableWriter = new HiveTableWriter()
  val workspace: String                = "yasp-core/src/test/resources/HiveTableWriter"

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

  test("write hive managed table") {
    hiveTableWriter.write(expectedDf, HiveTable("my_hive_table", Map.empty))
    val actual = spark.read.table("my_hive_table")
    assertDatasetEquals(actual, expectedDf)
  }

  test("write hive managed table overwrite") {
    hiveTableWriter.write(
      expectedDf,
      HiveTable("my_hive_table_2", Map.empty, mode = Some("overwrite"))
    )
    hiveTableWriter.write(
      expectedDf,
      HiveTable("my_hive_table_2", Map.empty, mode = Some("overwrite"))
    )
    assertDatasetEquals(spark.table("my_hive_table_2"), expectedDf)
  }

  test("write hive managed table append") {
    hiveTableWriter.write(
      expectedDf,
      HiveTable("my_hive_table_3", Map.empty, mode = Some("overwrite"))
    )
    hiveTableWriter.write(
      expectedDf,
      HiveTable("my_hive_table_3", Map.empty, mode = Some("append"))
    )
    assertDatasetEquals(spark.table("my_hive_table_3"), expectedDf.union(expectedDf))
  }

  test("write hive external table") {
    hiveTableWriter.write(
      expectedDf,
      HiveTable(
        "my_hive_table",
        Map("path" -> s"$workspace/my_hive_ext_table_1"),
        mode = Some("overwrite")
      )
    )
    val actual = spark.read.parquet(s"$workspace/my_hive_ext_table_1")
    assertDatasetEquals(actual, expectedDf)
  }

  test("write hive return left") {
    val actual = hiveTableWriter.write(
      expectedDf,
      HiveTable(
        "not_exists_table",
        Map("path" -> s"$workspace/xxxx"),
        mode = Some("append"),
        partitionBy = Seq("id")
      )
    )
    assert(actual.left.getOrElse(fail()).isInstanceOf[WriteError])
  }
}
