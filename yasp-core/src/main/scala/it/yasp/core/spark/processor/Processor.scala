package it.yasp.core.spark.processor

import it.yasp.core.spark.model.Process
import it.yasp.core.spark.model.Process.Sql
import org.apache.spark.sql.{Dataset, Row, SparkSession}

trait Processor[A <: Process] {
  def execute(process: A): Dataset[Row]
}

object Processor {

  class SqlProcessor(spark: SparkSession) extends Processor[Sql] {
    override def execute(process: Sql): Dataset[Row] =
      spark.sql(process.query)
  }

  class ProcessProcessor(sparkSession: SparkSession) extends Processor[Process] {
    override def execute(process: Process): Dataset[Row] =
      process match {
        case s @ Sql(_) => new SqlProcessor(sparkSession).execute(s)
      }
  }

}
