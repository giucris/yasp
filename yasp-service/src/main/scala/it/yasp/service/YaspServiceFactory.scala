package it.yasp.service

import it.yasp.core.spark.processor.Processor.ProcessProcessor
import it.yasp.core.spark.reader.Reader.SourceReader
import it.yasp.core.spark.registry.Registry.DefaultRegistry
import it.yasp.core.spark.writer.Writer.DestWriter
import it.yasp.loader.YaspLoader.DefaultYaspLoader
import it.yasp.processor.YaspProcessor.DefaultYaspProcessor
import it.yasp.service.YaspService.DefaultYaspService
import it.yasp.writer.YaspWriter.DefaultYaspWriter
import org.apache.spark.sql.SparkSession

class YaspServiceFactory {

  def create(spark: SparkSession): YaspService = {
    val registry = new DefaultRegistry(spark)
    new DefaultYaspService(
      new DefaultYaspLoader(new SourceReader(spark), registry),
      new DefaultYaspProcessor(new ProcessProcessor(spark), registry),
      new DefaultYaspWriter(registry, new DestWriter())
    )
  }
}
