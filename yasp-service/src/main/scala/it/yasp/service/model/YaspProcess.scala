package it.yasp.service.model

import it.yasp.core.spark.model.{CacheLayer, DataOperations, Process}

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
final case class YaspProcess(
    id: String,
    process: Process,
    partitions: Option[Int] = None,
    cache: Option[CacheLayer] = None
)

object YaspProcess {

  implicit class YaspProcessOps(yaspProcess: YaspProcess) {
    def dataOps: Option[DataOperations] =
      Some(DataOperations(yaspProcess.partitions, yaspProcess.cache))
        .flatMap {
          case DataOperations(None, None) => None
          case dataOps: DataOperations    => Some(dataOps)
        }
  }

}
