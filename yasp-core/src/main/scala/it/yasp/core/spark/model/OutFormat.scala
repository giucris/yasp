package it.yasp.core.spark.model

case class OutFormat(format:String, options:Map[String,String], partitionBy:Seq[String])
