package it.yasp.core.spark.model

final case class DataOperation(partitions: Option[Int], cache: Option[CacheLayer])
