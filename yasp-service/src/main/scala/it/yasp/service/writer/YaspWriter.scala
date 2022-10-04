package it.yasp.service.writer

import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.model.Dest
import it.yasp.core.spark.registry.Registry
import it.yasp.core.spark.writer.Writer
import it.yasp.service.err.YaspServiceError.YaspWriterError
import it.yasp.service.model.YaspSink

trait YaspWriter {
  def write(yaspSink: YaspSink): Either[YaspWriterError, Unit]
}

object YaspWriter {

  class DefaultYaspWriter(registry: Registry, writer: Writer[Dest])
      extends YaspWriter
      with StrictLogging {
    override def write(yaspSink: YaspSink): Either[YaspWriterError, Unit] = {
      logger.info(s"Sink: $yaspSink")
      for {
        ds <- registry.retrieve(yaspSink.id).leftMap(e => YaspWriterError(yaspSink, e))
        _  <- writer.write(ds, yaspSink.dest).leftMap(e => YaspWriterError(yaspSink, e))
      } yield ()
    }
  }
}
