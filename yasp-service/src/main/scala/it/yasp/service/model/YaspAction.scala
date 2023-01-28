package it.yasp.service.model

import it.yasp.core.spark.model.{CacheLayer, DataOperations, Dest, Process, Source}

sealed trait YaspAction extends Product with Serializable

object YaspAction {

  final case class YaspSource(
      id: String,
      dataset: String,
      source: Source,
      partitions: Option[Int],
      cache: Option[CacheLayer]
  ) extends YaspAction

  final case class YaspProcess(
      id: String,
      dataset: String,
      process: Process,
      partitions: Option[Int],
      cache: Option[CacheLayer]
  ) extends YaspAction

  final case class YaspSink(
      id: String,
      dataset: String,
      dest: Dest
  ) extends YaspAction

  implicit class YaspActionOps(yaspAction: YaspAction) {

    def dataOps: Option[DataOperations] = {
      val ops = yaspAction match {
        case x: YaspSource  => Some(DataOperations(x.partitions, x.cache))
        case x: YaspProcess => Some(DataOperations(x.partitions, x.cache))
        case _              => None
      }
      ops.flatMap {
        case DataOperations(None, None) => None
        case dataOps: DataOperations    => Some(dataOps)
      }
    }
  }
}

//
//package it.yasp.service.model
//
//import it.yasp.core.spark.model.{CacheLayer, DataOperations, Process}
//
///** A YaspProcess model
//  * @param id:
//  *   The unique ID of the process result
//  * @param process:
//  *   An instance of [[Process]]
//  * @param partitions:
//  *   An Optional number of partition that will be used to reshuffle the dataset
//  * @param cache:
//  *   An Optional [[CacheLayer]]
//  */
//final case class YaspProcess(
//                              id: String,
//                              dataset: String,
//                              process: Process,
//                              partitions: Option[Int] = None,
//                              cache: Option[CacheLayer] = None
//                            )
//

//
//}

//
//package it.yasp.service.model
//
//import it.yasp.core.spark.model.{CacheLayer, DataOperations, Source}
//
///** A YaspSource model
//  * @param id:
//  *   The unique ID of the source
//  * @param source:
//  *   An instance of [[Source]]
//  * @param partitions:
//  *   An Optional number of partition that will be used to reshuffle the dataset
//  * @param cache:
//  *   An Optional [[CacheLayer]]
//  */
//final case class YaspSource(
//                             id: String,
//                             dataset: String,
//                             source: Source,
//                             partitions: Option[Int] = None,
//                             cache: Option[CacheLayer] = None
//                           )
//
//object YaspSource {
//
//  implicit class YaspSourceOps(yaspSource: YaspSource) {
//    def dataOps: Option[DataOperations] =
//      Some(DataOperations(yaspSource.partitions, yaspSource.cache))
//        .flatMap {
//          case DataOperations(None, None) => None
//          case dataOps: DataOperations    => Some(dataOps)
//        }
//  }
//
//}
