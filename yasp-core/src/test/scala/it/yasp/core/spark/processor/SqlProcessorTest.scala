package it.yasp.core.spark.processor

import it.yasp.core.spark.model.Process.Sql
import it.yasp.core.spark.processor.Processor.SqlProcessor
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class SqlProcessorTest extends AnyFunSuite with SparkTestSuite {

  test("process") {
    spark
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
      .createTempView("ds")

    val actual   = new SqlProcessor(spark).execute(Sql("select ID from ds"))
    val expected = spark.createDataset(Seq(Row(1), Row(2), Row(3), Row(4)))(
      RowEncoder(StructType(Seq(StructField("ID", IntegerType, nullable = true))))
    )
    assertDatasetEquals(actual, expected)
  }

}
