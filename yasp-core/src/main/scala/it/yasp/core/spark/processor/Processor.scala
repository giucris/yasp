package it.yasp.core.spark.processor

import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.err.YaspCoreError.ProcessError
import it.yasp.core.spark.model.Process
import it.yasp.core.spark.model.Process.Sql
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Processor trait
  * Provide a method to execute a [[Process]]
  * @tparam A: type of [[Process]]
  */
trait Processor[A <: Process] {
  /**
    * Execute the provided process
    * @param process:
    * @return
    */
  def execute(process: A): Either[ProcessError, Dataset[Row]]
}

object Processor {

  /**
    * Sql processor instance
    *
    * @param spark: A [[SparkSession]] instance
    */
  class SqlProcessor(spark: SparkSession) extends Processor[Sql] with StrictLogging {
    override def execute(process: Sql): Either[ProcessError, Dataset[Row]] = {
      logger.info(s"Executing sql process: $process")
      try Right(spark.sql(process.query))
      catch { case t: Throwable => Left(ProcessError(process, t)) }
    }
  }

  class ProcessProcessor(sparkSession: SparkSession) extends Processor[Process] {
    override def execute(process: Process): Either[ProcessError, Dataset[Row]] =
      process match {
        case s @ Sql(_) => new SqlProcessor(sparkSession).execute(s)
      }
  }

}
