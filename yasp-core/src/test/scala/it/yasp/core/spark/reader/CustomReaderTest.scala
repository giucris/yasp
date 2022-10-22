package it.yasp.core.spark.reader

import it.yasp.core.spark.model.Source.Custom
import it.yasp.core.spark.plugin.{PluginProvider, ReaderPlugin}
import it.yasp.core.spark.reader.Reader.CustomReader
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.IntegerType
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.ClassTag

class CustomReaderTest extends AnyFunSuite with MockFactory with SparkTestSuite {

  val df: Dataset[Row] = spark.createDataset(Seq(Row(1, "x")))(
    RowEncoder(StructType(Seq(StructField("ID", IntegerType, nullable = true))))
  )


  test("read call a fails PluginProvider") {
    val pluginProvider = stub[PluginProvider]

    (pluginProvider
      .load[ReaderPlugin](_: String)(_: ClassTag[ReaderPlugin]))
      .when(*, *)
      .returns(Left(new IllegalArgumentException("x")))

    assert(new CustomReader(spark,pluginProvider).read(Custom("x", None)).isLeft)
  }

  test("read call ReaderPlugin read") {
    val readerPlugin   = mock[ReaderPlugin]
    val pluginProvider = stub[PluginProvider]

    (pluginProvider.load[ReaderPlugin](_: String)(_: ClassTag[ReaderPlugin])).when(*, *).returns(Right(readerPlugin))
    (readerPlugin.read _).expects(*, *).once().returns(df)

    assert(new CustomReader(spark, pluginProvider).read(Custom("x", None)).isRight)
  }

  test("read call fails ReaderPlugin read") {
    val readerPlugin   = mock[ReaderPlugin]
    val pluginProvider = stub[PluginProvider]

    (pluginProvider.load[ReaderPlugin](_: String)(_: ClassTag[ReaderPlugin])).when(*, *).returns(Right(readerPlugin))
    (readerPlugin.read _).expects(*, *).once().throws(new IllegalArgumentException("x"))

    assert(new CustomReader(spark, pluginProvider).read(Custom("x", None)).isLeft)
  }

}
