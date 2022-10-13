package it.yasp.core.spark.err

import it.yasp.core.spark.model.{CacheLayer, Dest, Process, Session, Source}

/** YaspCoreError Sum Type
  */
sealed abstract class YaspCoreError(val message: String, val cause: Throwable)
    extends Exception(message, cause)
    with Product
    with Serializable

object YaspCoreError {

  /** CreateSessionError that represent an exception during the Session initialization
    * @param session:
    *   [[Session]] model that was used to create the session
    * @param throwable:
    *   [[Throwable]] thrown
    */
  final case class CreateSessionError(
      session: Session,
      throwable: Throwable
  ) extends YaspCoreError(
        message = s"Unable to create session: $session",
        cause = throwable
      )

  /** ReadError that represent an exception during the read of a source
    * @param source:
    *   The [[Source]] config that was used to read
    * @param throwable:
    *   [[Throwable]] thrown
    */
  final case class ReadError(
      source: Source,
      throwable: Throwable
  ) extends YaspCoreError(
        message = s"Unable to read source: $source",
        cause = throwable
      )

  /** RegisterTableError that represent an exception during the registration of a dataset
    * @param table:
    *   [[String]] table name used to register the table
    * @param throwable:
    *   [[Throwable]] thrown
    */
  final case class RegisterTableError(
      table: String,
      throwable: Throwable
  ) extends YaspCoreError(
        message = s"Unable to register table : $table",
        cause = throwable
      )

  /** RetrieveTableError that represent an exception during the retrieve table
    * @param table:
    *   [[String]] table name used to retrieve the table
    * @param throwable:
    *   [[Throwable]] thrown
    */
  final case class RetrieveTableError(
      table: String,
      throwable: Throwable
  ) extends YaspCoreError(
        message = s"Unable to retrieve table : $table",
        cause = throwable
      )

  /** CacheOperationError that represent an exception during the cache operation.
    * @param cacheLayer:
    *   [[CacheLayer]] that was used to cache
    * @param throwable:
    *   [[Throwable]] thrown
    */
  final case class CacheOperationError(
      cacheLayer: CacheLayer,
      throwable: Throwable
  ) extends YaspCoreError(
        message = s"Unable to execute cache with CacheLayer: $cacheLayer",
        cause = throwable
      )

  /** RepartitionOperationError that represent an exception during the repartition operation
    * @param partitionNumber:
    *   [[Int]] partition number
    * @param throwable:
    *   [[Throwable]] thrown
    */
  final case class RepartitionOperationError(
      partitionNumber: Int,
      throwable: Throwable
  ) extends YaspCoreError(
        message = s"Unable to execute repartition with partition number: $partitionNumber",
        cause = throwable
      )

  /** ProcessError that represent an exception during the repartition operation
    * @param process:
    *   The [[Process]] that was executed
    * @param throwable:
    *   [[Throwable]] thrown
    */
  final case class ProcessError(
      process: Process,
      throwable: Throwable
  ) extends YaspCoreError(
        message = s"Unable to execute process: $process",
        cause = throwable
      )

  /** ProcessError that represent an exception during the repartition operation
    * @param dest:
    *   The [[Dest]] that was executed
    * @param throwable:
    *   [[Throwable]] thrown
    */
  final case class WriteError(
      dest: Dest,
      throwable: Throwable
  ) extends YaspCoreError(
        message = s"Unable to write data to dest: $dest",
        cause = throwable
      )

}
