package it.yasp.core.spark.operators

import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.err.YaspCoreError
import it.yasp.core.spark.err.YaspCoreError.{CacheOperationError, RepartitionOperationError}
import it.yasp.core.spark.model.CacheLayer._
import it.yasp.core.spark.model.{CacheLayer, DataOperations}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.storage.StorageLevel

/** DataOperators
  *
  * Provide a set of method to execute data operations
  */
class DataOperators extends StrictLogging {

  def exec(ds: Dataset[Row], dataOperation: DataOperations): Either[YaspCoreError, Dataset[Row]] =
    dataOperation match {
      case DataOperations(Some(p), Some(c)) => repartition(ds, p).flatMap(cache(_, c))
      case DataOperations(Some(p), None)    => repartition(ds, p)
      case DataOperations(None, Some(c))    => cache(ds, c)
      case DataOperations(None, None)       => Right(ds)
    }

  private def cache(ds: Dataset[Row], layer: CacheLayer): Either[CacheOperationError, Dataset[Row]] = {
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

  private def repartition(ds: Dataset[Row], partition: Int): Either[RepartitionOperationError, Dataset[Row]] = {
    logger.info(s"Applying dataset repartition with partition number: $partition")
    try Right(ds.repartition(partition))
    catch { case t: Throwable => Left(RepartitionOperationError(partition, t)) }
  }
}
