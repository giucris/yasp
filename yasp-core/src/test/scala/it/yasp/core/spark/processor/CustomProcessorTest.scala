package it.yasp.core.spark.processor

import it.yasp.core.spark.model.Process.Custom
import it.yasp.core.spark.plugin.PluginProvider
import it.yasp.core.spark.processor.Processor.CustomProcessor
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.{IntegerType, StringType}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.funsuite.AnyFunSuite

class CustomProcessorTest extends AnyFunSuite with SparkTestSuite {

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

  test("process with MyTestProcessorPlugin") {
    val processor = new CustomProcessor(spark, new PluginProvider)
    val actual    = processor.execute(Custom("it.yasp.core.spark.plugin.MyTestProcessorPlugin", None))
    assertDatasetEquals(actual.getOrElse(fail()), expectedDf)
  }

}
