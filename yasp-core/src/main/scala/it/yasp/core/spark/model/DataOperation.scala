package it.yasp.core.spark.model

case class DataOperation(partitions:Option[Int], cache: Option[CacheLayer])
