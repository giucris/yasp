package it.yasp.core.spark.model

final case class OutFormat(fmt: String, options: Map[String, String], partitionBy: Seq[String])
