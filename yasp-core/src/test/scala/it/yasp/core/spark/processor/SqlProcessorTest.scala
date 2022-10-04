package it.yasp.core.spark.processor

import it.yasp.core.spark.err.YaspCoreError.ProcessError
import it.yasp.core.spark.model.Process.Sql
import it.yasp.core.spark.processor.Processor.SqlProcessor
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.funsuite.AnyFunSuite

class SqlProcessorTest extends AnyFunSuite with SparkTestSuite {

  val sqlProcessor = new SqlProcessor(spark)
  val baseDf: Dataset[Row] = spark
    .createDataset(Seq(Row(1, "name1"), Row(2, "name2"), Row(3, "name3"), Row(4, "name4")))(
      RowEncoder(
        StructType(
          Seq(
            StructField("ID", IntegerType, nullable = true),
            StructField("NAME", StringType, nullable = true)
          )
        )
      )
    )

  test("process") {
    baseDf.createTempView("process_table_1")
    val actual   = sqlProcessor.execute(Sql("select ID from process_table_1"))
    val expected = spark.createDataset(Seq(Row(1), Row(2), Row(3), Row(4)))(
      RowEncoder(StructType(Seq(StructField("ID", IntegerType, nullable = true))))
    )
    assertDatasetEquals(actual.getOrElse(fail()), expected)
  }

  test("process return ProcessError") {
    val actual   = sqlProcessor.execute(Sql("select ID from process_table_x"))
    assert(actual.left.getOrElse(fail()).isInstanceOf[ProcessError])
  }

}
