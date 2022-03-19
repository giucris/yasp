package it.yasp.processor

import it.yasp.core.spark.model.Process
import it.yasp.core.spark.model.Process.Sql
import it.yasp.core.spark.processor.Processor
import it.yasp.core.spark.registry.Registry
import it.yasp.model.YaspProcess
import it.yasp.processor.YaspProcessor.DefaultYaspProcessor
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class YaspProcessorTest extends AnyFunSuite with SparkTestSuite with MockFactory {

  test("process") {
    val processor: Processor[Process] = mock[Processor[Process]]
    val registry: Registry            = mock[Registry]
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
        .expects(*, "my_processed_table")
        .once()
    )
    new DefaultYaspProcessor(processor, registry).process(
      YaspProcess("my_processed_table", Sql("select * from test_table"))
    )
  }

}
