package it.yasp.service.model

import it.yasp.core.spark.model.{CacheLayer, Process}

/** A YaspProcess model
  * @param id:
  *   The unique ID of the process result
  * @param process:
  *   An instance of [[Process]]
  * @param cache:
  *   An Optional [[CacheLayer]]
  */
case class YaspProcess(
    id: String,
    process: Process,
    cache: Option[CacheLayer],
    partitions: Option[Int] = None
)
