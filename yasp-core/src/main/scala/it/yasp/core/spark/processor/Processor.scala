package it.yasp.core.spark.processor

import it.yasp.core.spark.model.Process
import it.yasp.core.spark.model.Process.Sql
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Processor[A <: Process] {
  def execute(process: A): DataFrame
}

object Processor {

  class SqlProcessor(spark: SparkSession) extends Processor[Sql] {
    override def execute(process: Sql): DataFrame =
      spark.sql(process.query)
  }

}