package it.yasp.core.spark.model

/** CacheLayer
  */
sealed trait CacheLayer extends Product with Serializable

object CacheLayer {
  final case object Memory           extends CacheLayer
  final case object Disk             extends CacheLayer
  final case object MemoryAndDisk    extends CacheLayer
  final case object MemorySer        extends CacheLayer
  final case object MemoryAndDiskSer extends CacheLayer
  final case object Checkpoint       extends CacheLayer
}
