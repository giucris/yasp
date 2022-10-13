package it.yasp.core.spark.model

sealed trait IcebergCatalog extends Product with Serializable

object IcebergCatalog {

  final case class HiveIcebergCatalog(name: String, uri: String)                                 extends IcebergCatalog
  final case class HadoopIcebergCatalog(name: String, path: String)                              extends IcebergCatalog
  final case class CustomIcebergCatalog(name: String, impl: String, config: Map[String, String]) extends IcebergCatalog

}
