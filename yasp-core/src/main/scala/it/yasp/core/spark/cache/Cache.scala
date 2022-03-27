package it.yasp.core.spark.cache

import it.yasp.core.spark.model.CacheLayer
import it.yasp.core.spark.model.CacheLayer._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.storage.StorageLevel

/** Cache Provide a method to manage cache of a dataframe based on a specified [[CacheLayer]]
  */
trait Cache {

  /** Cache the provided dataset at the provided CacheLayer
    *
    * @param ds:
    *   input [[Dataset]]
    * @param layer:
    *   [[CacheLayer]]
    * @return
    *   cached [[Dataset]]
    */
  def cache(ds: Dataset[Row], layer: CacheLayer): Dataset[Row]
}

object Cache {

  class DefaultCache() extends Cache {
    override def cache(ds: Dataset[Row], layer: CacheLayer): Dataset[Row] =
      layer match {
        case Memory           => ds.persist(StorageLevel.MEMORY_ONLY)
        case Disk             => ds.persist(StorageLevel.DISK_ONLY)
        case MemoryAndDisk    => ds.persist(StorageLevel.MEMORY_AND_DISK)
        case MemorySer        => ds.persist(StorageLevel.MEMORY_ONLY_SER)
        case MemoryAndDiskSer => ds.persist(StorageLevel.MEMORY_AND_DISK_SER)
        case CheckPoint       => ds.checkpoint()
      }
  }

}