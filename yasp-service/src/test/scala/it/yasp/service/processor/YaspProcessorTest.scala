package it.yasp.service.processor

import it.yasp.core.spark.model.CacheLayer.Memory
import it.yasp.core.spark.model.Process
import it.yasp.core.spark.model.Process.Sql
import it.yasp.core.spark.operators.Operators
import it.yasp.core.spark.processor.Processor
import it.yasp.core.spark.registry.Registry
import it.yasp.service.model.YaspProcess
import it.yasp.service.processor.YaspProcessor.DefaultYaspProcessor
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class YaspProcessorTest extends AnyFunSuite with SparkTestSuite with MockFactory {
  val processor: Processor[Process] = mock[Processor[Process]]
  val operators: Operators          = mock[Operators]
  val registry: Registry            = mock[Registry]

  val yaspProcessor = new DefaultYaspProcessor(processor, operators, registry)

  test("process will exec sql proc and cache") {
    inSequence(
      (processor.execute _)
        .expects(Sql("select * from test_table"))
        .once()
        .returns(
          spark.createDataset(Seq(Row("a")))(
            RowEncoder(StructType(Seq(StructField("h1", StringType, nullable = true))))
          )
        ),
      (operators.cache _)
        .expects(*, Memory)
        .once()
        .returns(
          spark.createDataset(Seq(Row("a")))(
            RowEncoder(StructType(Seq(StructField("h1", StringType, nullable = true))))
          )
        ),
      (registry.register _)
        .expects(*, "tbl")
        .once()
    )
    yaspProcessor.process(YaspProcess("tbl", Sql("select * from test_table"), Some(Memory)))
  }

  test("process will exec sql proc and no cache") {
    inSequence(
      (processor.execute _)
        .expects(Sql("select * from test_table"))
        .once()
        .returns(
          spark.createDataset(Seq(Row("a")))(
            RowEncoder(StructType(Seq(StructField("h1", StringType, nullable = true))))
          )
        ),
      (registry.register _)
        .expects(*, "tbl")
        .once()
    )
    yaspProcessor.process(YaspProcess("tbl", Sql("select * from test_table"), None))
  }

}
