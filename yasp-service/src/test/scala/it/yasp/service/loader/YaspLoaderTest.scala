package it.yasp.service.loader

import it.yasp.core.spark.err.YaspCoreError.{CacheOperationError, RegisterTableError, RepartitionOperationError}
import it.yasp.core.spark.model.CacheLayer.Memory
import it.yasp.core.spark.model.{DataOperations, Source}
import it.yasp.core.spark.operators.DataOperators
import it.yasp.core.spark.reader.Reader
import it.yasp.core.spark.registry.Registry
import it.yasp.service.err.YaspServiceError.YaspLoaderError
import it.yasp.service.loader.YaspLoader.DefaultYaspLoader
import it.yasp.service.model.YaspSource
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class YaspLoaderTest extends AnyFunSuite with SparkTestSuite with MockFactory {

  val reader: Reader[Source]   = mock[Reader[Source]]
  val operators: DataOperators = mock[DataOperators]
  val registry: Registry       = mock[Registry]

  val yaspLoader: YaspLoader = new DefaultYaspLoader(reader, operators, registry)

  val baseDf: Dataset[Row] = spark.createDataset(Seq(Row("b")))(
    RowEncoder(StructType(Seq(StructField("h1", StringType, nullable = true))))
  )

  test("load read and register source") {
    inSequence(
      Seq(
        (reader.read _)
          .expects(Source.Format("parquet", options = Map("path" -> "x")))
          .once()
          .returns(Right(baseDf)),
        (registry.register _)
          .expects(*, "tbl")
          .once()
          .returns(Right(()))
      )
    )
    yaspLoader.load(
      YaspSource(
        id = "tbl",
        source = Source.Format("parquet", options = Map("path" -> "x"))
      )
    )
  }

  test("load read cache and register source") {
    inSequence(
      Seq(
        (reader.read _)
          .expects(Source.Format("parquet", options = Map("path" -> "x")))
          .once()
          .returns(Right(baseDf)),
        (operators.exec _)
          .expects(*, DataOperations(None, Some(Memory)))
          .once()
          .returns(Right(baseDf)),
        (registry.register _)
          .expects(*, "tbl")
          .once()
          .returns(Right(()))
      )
    )

    yaspLoader.load(
      YaspSource(
        id = "tbl",
        source = Source.Format("parquet", options = Map("path" -> "x")),
        cache = Some(Memory)
      )
    )
  }

  test("load read repartition cache and register a source") {
    inSequence(
      Seq(
        (reader.read _)
          .expects(Source.Format("parquet", options = Map("path" -> "x")))
          .once()
          .returns(Right(baseDf)),
        (operators.exec _)
          .expects(*, DataOperations(Some(100), Some(Memory)))
          .once()
          .returns(Right(baseDf)),
        (registry.register _)
          .expects(*, "tbl")
          .once()
          .returns(Right(()))
      )
    )

    yaspLoader.load(
      YaspSource(
        id = "tbl",
        source = Source.Format("parquet", options = Map("path" -> "x")),
        partitions = Some(100),
        cache = Some(Memory)
      )
    )
  }

  test("load return YaspLoaderError with RepartitionOperationError") {
    (reader.read _)
      .expects(*)
      .once()
      .returns(Right(baseDf))
    (operators.exec _)
      .expects(*, DataOperations(Some(100), None))
      .once()
      .returns(Left(RepartitionOperationError(10, new IllegalArgumentException())))

    val actual = yaspLoader.load(
      YaspSource(
        id = "tbl",
        source = Source.Format("parquet", options = Map("path" -> "x")),
        partitions = Some(100)
      )
    )
    assert(actual.left.getOrElse(fail()).isInstanceOf[YaspLoaderError])
  }

  test("load return YaspLoaderError with CacheOperationError") {
    (reader.read _)
      .expects(*)
      .once()
      .returns(Right(baseDf))
    (operators.exec _)
      .expects(*, DataOperations(None, Some(Memory)))
      .once()
      .returns(Left(CacheOperationError(Memory, new IllegalArgumentException())))

    val actual = yaspLoader.load(
      YaspSource(
        id = "tbl",
        source = Source.Format("parquet", options = Map("path" -> "x")),
        cache = Some(Memory)
      )
    )
    assert(actual.left.getOrElse(fail()).isInstanceOf[YaspLoaderError])
  }

  test("load return YaspLoaderError with RegisterTableError") {
    (reader.read _)
      .expects(*)
      .once()
      .returns(Right(baseDf))
    (registry.register _)
      .expects(*, *)
      .once()
      .returns(Left(RegisterTableError("x", new IllegalArgumentException())))

    val actual = yaspLoader.load(
      YaspSource(
        id = "tbl",
        source = Source.Format("parquet", options = Map("path" -> "x"))
      )
    )
    assert(actual.left.getOrElse(fail()).isInstanceOf[YaspLoaderError])
  }
}
