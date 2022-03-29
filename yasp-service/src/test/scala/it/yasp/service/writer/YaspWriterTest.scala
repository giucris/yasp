package it.yasp.service.writer

import it.yasp.core.spark.model.Dest
import it.yasp.core.spark.model.Dest._
import it.yasp.core.spark.registry.Registry
import it.yasp.core.spark.writer.Writer
import it.yasp.service.model.YaspSink
import it.yasp.service.writer.YaspWriter.DefaultYaspWriter
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class YaspWriterTest extends AnyFunSuite with SparkTestSuite with MockFactory {

  test("write") {
    val registry = mock[Registry]
    val writer   = mock[Writer[Dest]]

    inSequence(
      (registry.retrieve _)
        .expects("id")
        .once()
        .returns(
          spark.createDataset(Seq(Row("a")))(
            RowEncoder(StructType(Seq(StructField("h1", StringType, nullable = true))))
          )
        ),
      (writer.write _)
        .expects(*, Parquet("path", None))
        .once()
    )
    new DefaultYaspWriter(registry, writer).write(YaspSink("id", Parquet("path", None)))
  }

}
