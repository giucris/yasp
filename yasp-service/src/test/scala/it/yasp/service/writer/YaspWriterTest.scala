package it.yasp.service.writer

import it.yasp.core.spark.err.YaspCoreError.{RetrieveTableError, WriteError}
import it.yasp.core.spark.model.Dest
import it.yasp.core.spark.model.Dest._
import it.yasp.core.spark.registry.Registry
import it.yasp.core.spark.writer.Writer
import it.yasp.service.err.YaspServiceError.YaspWriterError
import it.yasp.service.model.YaspAction._
import it.yasp.service.writer.YaspWriter.DefaultYaspWriter
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class YaspWriterTest extends AnyFunSuite with SparkTestSuite with MockFactory {

  val registry: Registry            = mock[Registry]
  val writer: Writer[Dest]          = mock[Writer[Dest]]
  val yaspWriter: DefaultYaspWriter = new DefaultYaspWriter(registry, writer)

  val baseDf: Dataset[Row] = spark.createDataset(Seq(Row("a")))(
    RowEncoder(StructType(Seq(StructField("h1", StringType, nullable = true))))
  )

  test("write") {
    inSequence(
      Seq(
        (registry.retrieve _)
          .expects("dataset_1")
          .once()
          .returns(Right(baseDf)),
        (writer.write _)
          .expects(*, *)
          .once()
          .returns(Right(()))
      )
    )
    yaspWriter.write(YaspSink("id", dataset = "dataset_1", Format("parquet", Map("path" -> "path"))))
  }

  test("write return YaspWriterError on RetrieveTableError") {
    (registry.retrieve _)
      .expects(*)
      .once()
      .returns(Left(RetrieveTableError("x", new IllegalArgumentException())))

    val actual = yaspWriter.write(YaspSink("id", dataset = "dataset_1", Format("parquet", Map("path" -> "path"))))
    assert(actual.left.getOrElse(fail()).isInstanceOf[YaspWriterError])
  }

  test("write return YaspWriterError on WriteError") {
    inSequence(
      Seq(
        (registry.retrieve _)
          .expects(*)
          .once()
          .returns(Right(baseDf)),
        (writer.write _)
          .expects(*, *)
          .once()
          .returns(Left(WriteError(Dest.Format("x", Map.empty), new IllegalArgumentException())))
      )
    )
    val actual = yaspWriter.write(YaspSink("id", dataset = "dataset_1", Format("parquet", Map("path" -> "path"))))
    assert(actual.left.getOrElse(fail()).isInstanceOf[YaspWriterError])
  }

}
