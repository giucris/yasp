package it.yasp.service.executor

import it.yasp.service.loader.YaspLoader
import it.yasp.service.model.YaspPlan
import it.yasp.service.processor.YaspProcessor
import it.yasp.service.writer.YaspWriter

/** YaspExecutor
  *
  * Provide a method to execute a [[YaspPlan]]
  */
trait YaspExecutor {

  /** Execute a specific [[YaspPlan]]
    *
    * Load all [[it.yasp.service.model.YaspSource]], execute all
    * [[it.yasp.service.model.YaspProcess]] and write all [[it.yasp.service.model.YaspSink]]
    * @param yaspPlan:
    *   a [[YaspPlan]] instance to execute
    */
  def exec(yaspPlan: YaspPlan)
}

object YaspExecutor {

  def apply(loader: YaspLoader, processor: YaspProcessor, writer: YaspWriter): YaspExecutor =
    new DefaultYaspExecutor(loader, processor, writer)

  /** A YaspExecutor default implementation
    * @param loader:
    *   A [[YaspLoader]] instance that will load the [[it.yasp.service.model.YaspSource]]
    * @param processor:
    *   A [[YaspProcessor]] instance that will load the [[it.yasp.service.model.YaspProcess]]
    * @param writer:
    *   A [[YaspWriter]] instance that will wriet the [[it.yasp.service.model.YaspSink]]
    */
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
