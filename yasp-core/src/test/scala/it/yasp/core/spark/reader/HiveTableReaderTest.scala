package it.yasp.core.spark.reader

import it.yasp.core.spark.err.YaspCoreError.ReadError
import it.yasp.core.spark.model.Source.HiveTable
import it.yasp.core.spark.reader.Reader.HiveTableReader
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.{IntegerType, StringType}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class HiveTableReaderTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {

  val hiveReader               = new HiveTableReader(spark)
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

  test("read table") {
    expectedDf.write.saveAsTable("my_hive_table_xy")
    val actual = hiveReader.read(HiveTable("my_hive_table_xy"))
    assertDatasetEquals(actual.getOrElse(fail()), expectedDf)
  }

  test("read table return left") {
    val actual = hiveReader.read(HiveTable("yyyy"))
    assert(actual.left.getOrElse(fail()).isInstanceOf[ReadError])
  }
}
