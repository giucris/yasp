package it.yasp.core.spark.model

/** Dest Sum Type
  */
sealed trait Dest extends Product with Serializable

object Dest {

  /** Destination format. Mainly based on the spark format writer
    * @param format:
    *   Kind of format
    * @param options:
    *   Options configuration
    * @param mode:
    *   SaveMode
    * @param partitionBy:
    *   Seq of columns to partition the output
    */
  final case class Format(
      format: String,
      options: Map[String, String],
      mode: Option[String] = None,
      partitionBy: Seq[String] = Seq.empty
  ) extends Dest

}
