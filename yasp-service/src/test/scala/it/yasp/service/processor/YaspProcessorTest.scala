package it.yasp.service.processor

import it.yasp.core.spark.err.YaspCoreError.{
  CacheOperationError,
  ProcessError,
  RegisterTableError,
  RepartitionOperationError
}
import it.yasp.core.spark.model.CacheLayer.Memory
import it.yasp.core.spark.model.Process.Sql
import it.yasp.core.spark.model.{DataOperations, Process}
import it.yasp.core.spark.operators.DataOperators
import it.yasp.core.spark.processor.Processor
import it.yasp.core.spark.registry.Registry
import it.yasp.service.err.YaspServiceError.YaspProcessError
import it.yasp.service.model.YaspProcess
import it.yasp.service.processor.YaspProcessor.DefaultYaspProcessor
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class YaspProcessorTest extends AnyFunSuite with SparkTestSuite with MockFactory {
  val processor: Processor[Process] = mock[Processor[Process]]
  val operators: DataOperators      = mock[DataOperators]
  val registry: Registry            = mock[Registry]

  val yaspProcessor = new DefaultYaspProcessor(processor, operators, registry)

  val baseDf: Dataset[Row] = spark.createDataset(Seq(Row("a")))(
    RowEncoder(StructType(Seq(StructField("h1", StringType, nullable = true))))
  )

  test("process exec sql") {
    inSequence(
      Seq(
        (processor.execute _)
          .expects(Sql("select * from test_table"))
          .once()
          .returns(Right(baseDf)),
        (registry.register _)
          .expects(*, "tbl")
          .once()
          .returns(Right(()))
      )
    )
    yaspProcessor.process(YaspProcess("tbl", Sql("select * from test_table"), None))
  }

  test("process exec sql repartition and cache") {
    inSequence(
      Seq(
        (processor.execute _)
          .expects(Sql("select * from test_table"))
          .once()
          .returns(Right(baseDf)),
        (operators.exec _)
          .expects(*, DataOperations(Some(10), Some(Memory)))
          .once()
          .returns(Right(baseDf)),
        (registry.register _)
          .expects(*, "tbl")
          .once()
          .returns(Right(()))
      )
    )
    yaspProcessor.process(
      YaspProcess(
        "tbl",
        Sql("select * from test_table"),
        partitions = Some(10),
        cache = Some(Memory)
      )
    )
  }

  test("process return YaspProcessError with ProcessError") {
    (processor.execute _)
      .expects(*)
      .once()
      .returns(Left(ProcessError(Sql("x"), new IllegalArgumentException())))

    val actual = yaspProcessor.process(YaspProcess("x", Sql("x"), None))
    assert(actual.left.getOrElse(fail()).isInstanceOf[YaspProcessError])
  }

  test("process return YaspProcessError with RepartitionOperationError") {
    (processor.execute _)
      .expects(*)
      .once()
      .returns(Right(baseDf))
    (operators.exec _)
      .expects(*, DataOperations(Some(10), None))
      .once()
      .returns(Left(RepartitionOperationError(10, new IllegalArgumentException())))

    val actual = yaspProcessor.process(YaspProcess("x", Sql("x"), partitions = Some(10)))
    assert(actual.left.getOrElse(fail()).isInstanceOf[YaspProcessError])
  }

  test("process return YaspProcessError with CacheOperationError") {
    (processor.execute _)
      .expects(*)
      .once()
      .returns(Right(baseDf))
    (operators.exec _)
      .expects(*, DataOperations(None, Some(Memory)))
      .once()
      .returns(Left(CacheOperationError(Memory, new IllegalArgumentException())))

    val actual = yaspProcessor.process(YaspProcess("x", Sql("x"), cache = Some(Memory)))
    assert(actual.left.getOrElse(fail()).isInstanceOf[YaspProcessError])
  }

  test("process return YaspProcessError with RegisterTableError") {
    (processor.execute _)
      .expects(*)
      .once()
      .returns(Right(baseDf))
    (registry.register _)
      .expects(*, *)
      .once()
      .returns(Left(RegisterTableError("x", new IllegalArgumentException())))

    val actual = yaspProcessor.process(YaspProcess("x", Sql("x"), None))
    assert(actual.left.getOrElse(fail()).isInstanceOf[YaspProcessError])
  }
}
