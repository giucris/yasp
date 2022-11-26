package it.yasp.service.model

import it.yasp.core.spark.model.{DataOperation, Process}

/** A YaspProcess model
  * @param id:
  *   The unique ID of the process result
  * @param process:
  *   An instance of [[Process]]
  * @param dataOps:
  *   An Optional instance of [[DataOperation]]
  */
final case class YaspProcess(
    id: String,
    process: Process,
    dataOps: Option[DataOperation]
)
