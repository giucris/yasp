package it.yasp.service.processor

import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.model.Process
import it.yasp.core.spark.operators.Operators
import it.yasp.core.spark.processor.Processor
import it.yasp.core.spark.registry.Registry
import it.yasp.service.model.YaspProcess

/** YaspProcessor
  *
  * Provide a unified way to execute a [[YaspProcess]]
  */
trait YaspProcessor {

  /** Execute the provided [[YaspProcess]] cache the result if some cache option are specified on
    * the process and register the result as a table.
    * @param process:
    *   A [[YaspProcess]] instance
    */
  def process(process: YaspProcess)
}

object YaspProcessor {

  /** DefaultYaspProcessor implementation
    *
    * @param processor
    *   : A [[Processor]] instance
    * @param operators
    *   : A [[Registry]] instance
    * @param registry
    *   : A [[Operators]] instance
    */
  class DefaultYaspProcessor(
      processor: Processor[Process],
      operators: Operators,
      registry: Registry
  ) extends YaspProcessor with StrictLogging{
    override def process(process: YaspProcess): Unit = {
      logger.info(s"Process: $process")
      val ds1 = processor.execute(process.process)
      val ds2 = process.partitions.map(operators.repartition(ds1, _)).getOrElse(ds1)
      val ds3 = process.cache.map(operators.cache(ds2, _)).getOrElse(ds2)
      registry.register(ds3, process.id)
    }
  }
}
