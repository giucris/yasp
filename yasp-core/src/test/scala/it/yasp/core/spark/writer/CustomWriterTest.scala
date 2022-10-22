package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest.Custom
import it.yasp.core.spark.plugin.{PluginProvider, WriterPlugin}
import it.yasp.core.spark.writer.Writer.CustomWriter
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.{IntegerType, StringType}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.ClassTag

class CustomWriterTest extends AnyFunSuite with MockFactory with SparkTestSuite {
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

  test("write call a fails PluginProvider") {
    val pluginProvider = stub[PluginProvider]

    (pluginProvider
      .load[WriterPlugin](_: String)(_: ClassTag[WriterPlugin]))
      .when(*, *)
      .returns(Left(new IllegalArgumentException("x")))

    assert(new CustomWriter(pluginProvider).write(expectedDf, Custom("x", None)).isLeft)
  }

  test("write call a WriterPlugin") {
    val writerPlugin   = mock[WriterPlugin]
    val pluginProvider = stub[PluginProvider]

    (pluginProvider.load[WriterPlugin](_: String)(_: ClassTag[WriterPlugin])).when(*, *).returns(Right(writerPlugin))
    (writerPlugin.write _).expects(*, *).once().returns(())

    assert(new CustomWriter(pluginProvider).write(expectedDf, Custom("y", None)).isRight)
  }

  test("write call a fails WriterPlugin") {
    val writerPlugin   = mock[WriterPlugin]
    val pluginProvider = stub[PluginProvider]

    (pluginProvider.load[WriterPlugin](_: String)(_: ClassTag[WriterPlugin])).when(*, *).returns(Right(writerPlugin))
    (writerPlugin.write _).expects(*, *).once().throws(new IllegalArgumentException("x"))

    assert(new CustomWriter(pluginProvider).write(expectedDf, Custom("z", None)).isLeft)
  }

}
