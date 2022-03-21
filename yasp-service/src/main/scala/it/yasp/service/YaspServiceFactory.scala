package it.yasp.service

import it.yasp.core.spark.processor.Processor
import it.yasp.core.spark.processor.Processor.{ProcessProcessor, SqlProcessor}
import it.yasp.core.spark.reader.Reader.SourceReader
import it.yasp.core.spark.registry.Registry.DefaultRegistry
import it.yasp.core.spark.writer.Writer.DestWriter
import it.yasp.loader.YaspLoader.DefaultYaspLoader
import it.yasp.processor.YaspProcessor.DefaultYaspProcessor
import it.yasp.service.YaspService.DefaultYaspService
import it.yasp.writer.YaspWriter.DefaultYaspWriter
import it.yasp.core.spark.model.Process
import org.apache.spark.sql.SparkSession

class YaspServiceFactory {

  def create(spark:SparkSession): YaspService = {
    val registry = new DefaultRegistry(spark)
    val reader = new SourceReader(spark)
    val processor = new ProcessProcessor(spark)
    val writer = new DestWriter()
    new DefaultYaspService(
      new DefaultYaspLoader(reader,registry),
      new DefaultYaspProcessor(processor,registry),
      new DefaultYaspWriter(registry,writer)
    )
  }
}
