package it.yasp.service.loader

import it.yasp.core.spark.cache.Cache
import it.yasp.core.spark.model.Source
import it.yasp.core.spark.reader.Reader
import it.yasp.core.spark.registry.Registry
import it.yasp.service.model.YaspSource

/** YaspLoader
  *
  * Provide a unified way to load a [[YaspSource]]
  */
trait YaspLoader {

  /** Read the specified YaspSource, cache the result if some cache specification exists on the
    * source and then register the table
    * @param source:
    *   A [[YaspSource]] instance
    */
  def load(source: YaspSource): Unit
}

object YaspLoader {

  /** DefaultYaspLoader Implementation
    * @param reader:
    *   A [[Reader]] instance
    * @param registry:
    *   A [[Registry]] instance
    * @param cache:
    *   A [[Cache]] instance
    */
  class DefaultYaspLoader(reader: Reader[Source], registry: Registry, cache: Cache)
      extends YaspLoader {
    override def load(source: YaspSource): Unit = {
      val ds1 = reader.read(source.source)
      val ds2 = source.partitions.map(ds1.repartition).getOrElse(ds1)
      val ds3 = source.cache.map(cache.cache(ds2, _)).getOrElse(ds1)
      registry.register(ds3, source.id)
    }
  }
}
