package it.yasp.core.spark.operators

import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.err.YaspCoreError.{CacheOperationError, RepartitionOperationError}
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
    *   Right([[Dataset]]) if cache operation is successful completed Left([[CacheOperationError]])
    *   if cache operation raise some exception
    */
  def cache(ds: Dataset[Row], layer: CacheLayer): Either[CacheOperationError, Dataset[Row]]

  /** Repartition the provided dataset into the provided number of partitions
    *
    * @param ds:
    *   input [[Dataset]]
    * @param partition:
    *   number of partition
    * @return
    *   Right([[Dataset]]) if repartition operation is successful completed
    *   Left([[RepartitionOperationError]]) if repartition operation raise some exception
    */
  def repartition(ds: Dataset[Row], partition: Int): Either[RepartitionOperationError, Dataset[Row]]
}

object Operators {

  /** DefaultCache implementation
    */
  class DefaultOperators extends Operators with StrictLogging {

    override def cache(
        ds: Dataset[Row],
        layer: CacheLayer
    ): Either[CacheOperationError, Dataset[Row]] = {
      logger.info(s"Applying the cache layer: $layer to the provided Dataset")
      try layer match {
        case Memory           => Right(ds.persist(StorageLevel.MEMORY_ONLY))
        case Disk             => Right(ds.persist(StorageLevel.DISK_ONLY))
        case MemoryAndDisk    => Right(ds.persist(StorageLevel.MEMORY_AND_DISK))
        case MemorySer        => Right(ds.persist(StorageLevel.MEMORY_ONLY_SER))
        case MemoryAndDiskSer => Right(ds.persist(StorageLevel.MEMORY_AND_DISK_SER))
        case Checkpoint       => Right(ds.checkpoint())
      } catch { case t: Throwable => Left(CacheOperationError(layer, t)) }
    }

    override def repartition(
        ds: Dataset[Row],
        partition: Int
    ): Either[RepartitionOperationError, Dataset[Row]] = {
      logger.info(s"Applying dataset repartition with partition number: $partition")
      try Right(ds.repartition(partition))
      catch { case t: Throwable => Left(RepartitionOperationError(partition, t)) }
    }
  }
}
