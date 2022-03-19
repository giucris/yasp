package it.yasp.writer

import it.yasp.core.spark.model.Dest
import it.yasp.core.spark.registry.Registry
import it.yasp.core.spark.writer.Writer
import it.yasp.model.YaspSink

trait YaspWriter {
  def write(yaspSink: YaspSink): Unit
}

object YaspWriter {

  class DefaultYaspWriter(registry: Registry, writer: Writer[Dest]) extends YaspWriter {
    override def write(yaspSink: YaspSink): Unit = {
      val df = registry.retrieve(yaspSink.id)
      writer.write(df, yaspSink.dest)
    }
  }
}
