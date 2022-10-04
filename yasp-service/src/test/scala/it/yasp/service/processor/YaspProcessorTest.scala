package it.yasp.service.processor

import it.yasp.core.spark.err.YaspCoreError.{
  CacheOperationError,
  ProcessError,
  RegisterTableError,
  RepartitionOperationError
}
import it.yasp.core.spark.model.CacheLayer.Memory
import it.yasp.core.spark.model.Process
import it.yasp.core.spark.model.Process.Sql
import it.yasp.core.spark.operators.Operators
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
  val operators: Operators          = mock[Operators]
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
    yaspProcessor.process(YaspProcess("tbl", Sql("select * from test_table"), cache = None))
  }

  test("process exec sql repartition and cache") {
    inSequence(
      Seq(
        (processor.execute _)
          .expects(Sql("select * from test_table"))
          .once()
          .returns(Right(baseDf)),
        (operators.repartition _)
          .expects(*, 10)
          .once()
          .returns(Right(baseDf)),
        (operators.cache _)
          .expects(*, Memory)
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
        cache = Some(Memory),
        partitions = Some(10)
      )
    )
  }

  test("process return YaspProcessError with ProcessError") {
    (processor.execute _)
      .expects(*)
      .once()
      .returns(Left(ProcessError(Sql("x"), new IllegalArgumentException())))

    val actual = yaspProcessor.process(YaspProcess("x", Sql("x")))
    assert(actual.left.getOrElse(fail()).isInstanceOf[YaspProcessError])
  }

  test("process return YaspProcessError with RepartitionOperationError") {
    (processor.execute _)
      .expects(*)
      .once()
      .returns(Right(baseDf))
    (operators.repartition _)
      .expects(*, 10)
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
    (operators.cache _)
      .expects(*, *)
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

    val actual = yaspProcessor.process(YaspProcess("x", Sql("x")))
    assert(actual.left.getOrElse(fail()).isInstanceOf[YaspProcessError])
  }
}
