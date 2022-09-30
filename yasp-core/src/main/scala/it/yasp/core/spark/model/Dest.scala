package it.yasp.core.spark.model

/** Dest Sum Type
  */
sealed trait Dest extends Product with Serializable

object Dest {

  final case class Format(
      format:String,
      options:Map[String,String],
      mode:Option[String] = None,
      partitionBy: Seq[String] = Seq.empty
  ) extends Dest

}
