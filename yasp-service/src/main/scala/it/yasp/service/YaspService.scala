package it.yasp.service

import it.yasp.loader.YaspLoader
import it.yasp.model.{YaspProcess, YaspSink, YaspSource}
import it.yasp.processor.YaspProcessor
import it.yasp.writer.YaspWriter

trait YaspService {
  def exec(sources: Seq[YaspSource], processes: Seq[YaspProcess], sinks: Seq[YaspSink])
}

object YaspService {

  class DefaultYaspService(
      loader: YaspLoader,
      processor: YaspProcessor,
      writer: YaspWriter
  ) extends YaspService {

    override def exec(
        sources: Seq[YaspSource],
        processes: Seq[YaspProcess],
        sinks: Seq[YaspSink]
    ): Unit = {
      sources.foreach(loader.load)
      processes.foreach(processor.process)
      sinks.foreach(writer.write)
    }

  }

}
