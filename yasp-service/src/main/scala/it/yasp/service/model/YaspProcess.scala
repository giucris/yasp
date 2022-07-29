package it.yasp.service.model

import it.yasp.core.spark.model.{CacheLayer, Process}

/** A YaspProcess model
  * @param id:
  *   The unique ID of the process result
  * @param process:
  *   An instance of [[Process]]
  * @param partitions:
  *   An Optional number of partition that will be used to reshuffle the dataset
  * @param cache:
  *   An Optional [[CacheLayer]]
  */
case class YaspProcess(
    id: String,
    process: Process,
    partitions: Option[Int] = None,
    cache: Option[CacheLayer]
)
