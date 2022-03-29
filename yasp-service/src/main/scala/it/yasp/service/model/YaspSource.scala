package it.yasp.service.model

import it.yasp.core.spark.model.{CacheLayer, Source}

/** A YaspSource model
  * @param id:
  *   The unique ID of the source
  * @param source:
  *   An instance of [[Source]]
  * @param cache:
  *   An Optional [[CacheLayer]]
  */
case class YaspSource(
    id: String,
    source: Source,
    cache: Option[CacheLayer]
)
