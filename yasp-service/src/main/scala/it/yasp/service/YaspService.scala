package it.yasp.service

import it.yasp.loader.YaspLoader
import it.yasp.model.{YaspPlan, YaspProcess, YaspSink, YaspSource}
import it.yasp.processor.YaspProcessor
import it.yasp.writer.YaspWriter

trait YaspService {
  def run(yaspPlan: YaspPlan)
}

object YaspService {

  class DefaultYaspService(
      loader: YaspLoader,
      processor: YaspProcessor,
      writer: YaspWriter
  ) extends YaspService {

    override def run(yaspPlan: YaspPlan): Unit = {
      yaspPlan.sources.foreach(loader.load)
      yaspPlan.processes.foreach(processor.process)
      yaspPlan.sinks.foreach(writer.write)
    }

  }

}
