package it.yasp.service

import it.yasp.service.loader.YaspLoader
import it.yasp.service.model.YaspPlan
import it.yasp.service.processor.YaspProcessor
import it.yasp.service.writer.YaspWriter

trait YaspExecutor {
  def exec(yaspPlan: YaspPlan)
}

object YaspExecutor {

  class DefaultYaspExecutor(
      loader: YaspLoader,
      processor: YaspProcessor,
      writer: YaspWriter
  ) extends YaspExecutor {

    override def exec(yaspPlan: YaspPlan): Unit = {
      yaspPlan.sources.foreach(loader.load)
      yaspPlan.processes.foreach(processor.process)
      yaspPlan.sinks.foreach(writer.write)
    }

  }

}
