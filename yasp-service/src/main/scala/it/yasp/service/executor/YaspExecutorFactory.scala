package it.yasp.service.executor

import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.operators.DataOperators
import it.yasp.core.spark.processor.Processor.ProcessProcessor
import it.yasp.core.spark.reader.Reader.SourceReader
import it.yasp.core.spark.registry.Registry.DefaultRegistry
import it.yasp.core.spark.writer.Writer.DestWriter
import it.yasp.service.executor.YaspExecutor.DefaultYaspExecutor
import it.yasp.service.loader.YaspLoader.DefaultYaspLoader
import it.yasp.service.processor.YaspProcessor.DefaultYaspProcessor
import it.yasp.service.writer.YaspWriter.DefaultYaspWriter
import org.apache.spark.sql.SparkSession

/** YaspServiceFactory
  *
  * Provide a method to create a YaspService at runtime
  */
class YaspExecutorFactory extends StrictLogging {

  /** Create a YaspService
    *
    * @param spark:
    *   A SparkSession instance
    * @return
    *   A [[YaspExecutor]]
    */
  def create(spark: SparkSession): YaspExecutor = {
    logger.info("Create YaspExecutor instance")
    val registry      = new DefaultRegistry(spark)
    val dataOperators = new DataOperators()
    new DefaultYaspExecutor(
      new DefaultYaspLoader(new SourceReader(spark), dataOperators, registry),
      new DefaultYaspProcessor(new ProcessProcessor(spark), dataOperators, registry),
      new DefaultYaspWriter(registry, new DestWriter())
    )
  }
}
