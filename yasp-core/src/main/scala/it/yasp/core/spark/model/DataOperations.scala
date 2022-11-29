package it.yasp.core.spark.model

final case class DataOperations(partitions: Option[Int], cache: Option[CacheLayer])
