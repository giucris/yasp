package it.yasp.core.spark.model

case class OutFormat(fmt:String, options:Map[String,String], partitionBy:Seq[String])
