package it.yasp.service.writer

import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.model.Dest
import it.yasp.core.spark.registry.Registry
import it.yasp.core.spark.writer.Writer
import it.yasp.service.model.YaspSink

trait YaspWriter {
  def write(yaspSink: YaspSink): Unit
}

object YaspWriter {

  class DefaultYaspWriter(registry: Registry, writer: Writer[Dest]) extends YaspWriter with StrictLogging{
    override def write(yaspSink: YaspSink): Unit = {
      logger.info(s"Sink: $yaspSink")
      val df = registry.retrieve(yaspSink.id)
      writer.write(df, yaspSink.dest)
    }
  }
}
