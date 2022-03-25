package it.yasp.service

import it.yasp.service.loader.YaspLoader
import it.yasp.service.model.YaspPlan
import it.yasp.service.processor.YaspProcessor
import it.yasp.service.writer.YaspWriter

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
