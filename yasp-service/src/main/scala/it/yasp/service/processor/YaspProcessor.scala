package it.yasp.service.processor

import it.yasp.core.spark.cache.Cache
import it.yasp.core.spark.model.Process
import it.yasp.core.spark.processor.Processor
import it.yasp.core.spark.registry.Registry
import it.yasp.service.model.YaspProcess

/** YaspProcessor
  *
  * Provide a unified way to execute a [[YaspProcess]]
  */
trait YaspProcessor {
  /**
    * Execute the provided [[YaspProcess]] cache the result if some cache option are specified on the process and register the result as a table.
    * @param process: A [[YaspProcess]] instance
    */
  def process(process: YaspProcess)
}

object YaspProcessor {

  /**
    * DefaultYaspProcessor implementation
    * @param processor: A [[Processor]] instance
    * @param registry: A [[Registry]] instance
    * @param cache: A [[Cache]] instance
    */
  class DefaultYaspProcessor(processor: Processor[Process], registry: Registry, cache: Cache)
      extends YaspProcessor {
    override def process(process: YaspProcess): Unit = {
      val df1 = processor.execute(process.process)
      val df2 = process.cache.map(cache.cache(df1, _)).getOrElse(df1)
      registry.register(df2, process.id)
    }
  }
}
