package it.yasp.service.model

import it.yasp.core.spark.model.{DataOperation, Source}

/** A YaspSource model
  * @param id:
  *   The unique ID of the source
  * @param source:
  *   An instance of [[Source]]
  * @param dataOps:
  *   An Optional instance of [[DataOperation]]
  */
final case class YaspSource(
    id: String,
    source: Source,
    dataOps: Option[DataOperation]
)
