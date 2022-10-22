package it.yasp.core.spark.processor

import it.yasp.core.spark.model.Process.Custom
import it.yasp.core.spark.plugin.{PluginProvider, ProcessorPlugin}
import it.yasp.core.spark.processor.Processor.CustomProcessor
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.IntegerType
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.ClassTag

class CustomProcessorTest extends AnyFunSuite with MockFactory with SparkTestSuite {

  val df: Dataset[Row] = spark.createDataset(Seq(Row(1, "x")))(
    RowEncoder(StructType(Seq(StructField("ID", IntegerType, nullable = true))))
  )

  test("execute call a fails PluginProvider") {
    val pluginProvider = stub[PluginProvider]

    (pluginProvider
      .load[ProcessorPlugin](_: String)(_: ClassTag[ProcessorPlugin]))
      .when(*, *)
      .returns(Left(new IllegalArgumentException("x")))

    assert(new CustomProcessor(spark, pluginProvider).execute(Custom("x", None)).isLeft)
  }

  test("execute call ProcessorPlugin read") {
    val processorPlugin = mock[ProcessorPlugin]
    val pluginProvider  = stub[PluginProvider]

    (pluginProvider
      .load[ProcessorPlugin](_: String)(_: ClassTag[ProcessorPlugin]))
      .when(*, *)
      .returns(Right(processorPlugin))
    (processorPlugin.process _).expects(*, *).once().returns(df)

    assert(new CustomProcessor(spark, pluginProvider).execute(Custom("x", None)).isRight)
  }

  test("execute call fails ProcessorPlugin process") {
    val processorPlugin = mock[ProcessorPlugin]
    val pluginProvider  = stub[PluginProvider]

    (pluginProvider
      .load[ProcessorPlugin](_: String)(_: ClassTag[ProcessorPlugin]))
      .when(*, *)
      .returns(Right(processorPlugin))
    (processorPlugin.process _).expects(*, *).once().throws(new IllegalArgumentException("x"))

    assert(new CustomProcessor(spark, pluginProvider).execute(Custom("x", None)).isLeft)
  }
}
