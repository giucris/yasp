package it.yasp.service.model

import it.yasp.core.spark.model.{CacheLayer, DataOperations, Dest, Process, Source}

sealed trait YaspAction extends Product with Serializable {
  def id:String
  def dataset:String
  def dependsOn:Option[Seq[String]]
}

object YaspAction {

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
      dataset: String,
      source: Source,
      partitions: Option[Int],
      cache: Option[CacheLayer],
      dependsOn : Option[Seq[String]] = None
  ) extends YaspAction

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
      dataset: String,
      process: Process,
      partitions: Option[Int],
      cache: Option[CacheLayer],
      dependsOn : Option[Seq[String]]=None,
  ) extends YaspAction

  /** YaspSink model
    * @param id:
    *   a [[String]] id of the data to store on the specific Dest
    * @param dest:
    *   a [[Dest]] instance
    */
  final case class YaspSink(
      id: String,
      dataset: String,
      dest: Dest,
      dependsOn : Option[Seq[String]]=None
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
