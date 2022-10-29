package it.yasp.core.spark.model

case class DataOperation(partition:Option[Int], cacheLayer: Option[CacheLayer])
