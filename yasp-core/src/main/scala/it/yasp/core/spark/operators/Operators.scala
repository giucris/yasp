package it.yasp.core.spark.operators

import it.yasp.core.spark.model.CacheLayer
import it.yasp.core.spark.model.CacheLayer._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.storage.StorageLevel

/** Operators
  *
  * Provide a set of method to execute data operations
  */
trait Operators {

  /** Cache the provided dataset into a specific [[CacheLayer]]
    *
    * @param ds:
    *   input [[Dataset]]
    * @param layer:
    *   [[CacheLayer]]
    * @return
    *   cached [[Dataset]]
    */
  def cache(ds: Dataset[Row], layer: CacheLayer): Dataset[Row]

  /** Repartition the provided dataset into the provided number of partitions
    *
    * @param ds:
    *   input [[Dataset]]
    * @param partition:
    *   number of partition
    * @return
    *   cached [[Dataset]]
    */
  def repartition(ds: Dataset[Row], partition: Int): Dataset[Row]
}

object Operators {

  /** DefaultCache implementation
    */
  class DefaultOperators extends Operators {
    override def cache(ds: Dataset[Row], layer: CacheLayer): Dataset[Row] =
      layer match {
        case Memory           => ds.persist(StorageLevel.MEMORY_ONLY)
        case Disk             => ds.persist(StorageLevel.DISK_ONLY)
        case MemoryAndDisk    => ds.persist(StorageLevel.MEMORY_AND_DISK)
        case MemorySer        => ds.persist(StorageLevel.MEMORY_ONLY_SER)
        case MemoryAndDiskSer => ds.persist(StorageLevel.MEMORY_AND_DISK_SER)
        case Checkpoint       => ds.checkpoint()
      }

    override def repartition(ds: Dataset[Row], partition: Int): Dataset[Row] =
      ds.repartition(partition)
  }
}
