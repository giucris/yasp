package it.yasp.service

import it.yasp.core.spark.cache.Cache.DefaultCache
import it.yasp.core.spark.processor.Processor.ProcessProcessor
import it.yasp.core.spark.reader.Reader.SourceReader
import it.yasp.core.spark.registry.Registry.DefaultRegistry
import it.yasp.core.spark.writer.Writer.DestWriter
import it.yasp.service.YaspExecutor.DefaultYaspExecutor
import it.yasp.service.loader.YaspLoader.DefaultYaspLoader
import it.yasp.service.processor.YaspProcessor.DefaultYaspProcessor
import it.yasp.service.writer.YaspWriter.DefaultYaspWriter
import org.apache.spark.sql.SparkSession

/** YaspServiceFactory
  *
  * Provide a method to create a YaspService at runtime
  */
class YaspExecutorFactory {

  /** Create a YaspService
 *
    * @param spark:
    *   A SparkSession instance
    * @return
    * A [[YaspExecutor]]
    */
  def create(spark: SparkSession): YaspExecutor = {
    val registry = new DefaultRegistry(spark)
    val cache    = new DefaultCache()
    new DefaultYaspExecutor(
      new DefaultYaspLoader(new SourceReader(spark), registry, cache),
      new DefaultYaspProcessor(new ProcessProcessor(spark), registry, cache),
      new DefaultYaspWriter(registry, new DestWriter())
    )
  }
}