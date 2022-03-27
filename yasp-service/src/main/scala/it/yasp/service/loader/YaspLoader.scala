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
    * @param source: A [[YaspSource]] instance
    */
  def load(source: YaspSource): Unit
}

object YaspLoader {

  /** DefaultYaspLoader Implementation
    * @param reader:
    *   An instance [[Reader]]
    * @param registry:
    *   An instance of Registry
    * @param cache:
    *   An instance of Cache
    */
  class DefaultYaspLoader(reader: Reader[Source], registry: Registry, cache: Cache)
      extends YaspLoader {
    override def load(source: YaspSource): Unit = {
      val src = reader.read(source.source)
      val ds  = source.cache.map(cache.cache(src, _)).getOrElse(src)
      registry.register(ds, source.id)
    }
  }
}
