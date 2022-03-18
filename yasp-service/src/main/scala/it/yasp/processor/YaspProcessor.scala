package it.yasp.processor

import it.yasp.core.spark.model.Process
import it.yasp.core.spark.processor.Processor
import it.yasp.core.spark.registry.Registry
import it.yasp.model.YaspProcess

trait YaspProcessor {
  def process(process: YaspProcess)
}

object YaspProcessor {

  class DefaultYaspProcessor(processor: Processor[Process], registry: Registry)
      extends YaspProcessor {
    override def process(process: YaspProcess): Unit = {
      val df = processor.execute(process.process)
      registry.register(df, process.id)
    }
  }
}
