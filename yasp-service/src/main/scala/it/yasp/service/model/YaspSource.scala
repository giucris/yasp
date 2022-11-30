package it.yasp.service.model

import it.yasp.core.spark.model.{CacheLayer, DataOperations, Source}

/** A YaspSource model
  * @param id:
  *   The unique ID of the source
  * @param source:
  *   An instance of [[Source]]
  * @param partitions:
  *   An Optional number of partition that will be used to reshuffle the dataset
  * @param cache:
  *   An Optional [[CacheLayer]]
  */
final case class YaspSource(
    id: String,
    source: Source,
    partitions: Option[Int] = None,
    cache: Option[CacheLayer] = None
)

object YaspSource {

  implicit class YaspSourceOps(yaspSource: YaspSource) {
    def dataOps: Option[DataOperations] =
      Some(DataOperations(yaspSource.partitions, yaspSource.cache))
        .flatMap {
          case DataOperations(None, None) => None
          case dataOps: DataOperations    => Some(dataOps)
        }
  }

}
