package it.yasp.core.spark.processor

import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.err.YaspCoreError.ProcessError
import it.yasp.core.spark.model.Process
import it.yasp.core.spark.model.Process.{Custom, Sql}
import it.yasp.core.spark.plugin.{PluginProvider, ProcessorPlugin}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/** Processor trait Provide a method to execute a [[Process]]
  * @tparam A:
  *   type of [[Process]]
  */
trait Processor[A <: Process] {

  /** Execute the provided process
    * @param process:
    * @return
    */
  def execute(process: A): Either[ProcessError, Dataset[Row]]
}

object Processor {

  /** Sql processor instance
    *
    * @param spark:
    *   A [[SparkSession]] instance
    */
  class SqlProcessor(spark: SparkSession) extends Processor[Sql] with StrictLogging {
    override def execute(process: Sql): Either[ProcessError, Dataset[Row]] = {
      logger.info(s"Executing sql process: $process")
      try Right(spark.sql(process.query))
      catch { case t: Throwable => Left(ProcessError(process, t)) }
    }
  }

  class CustomProcessor(sparkSession: SparkSession, pluginProvider: PluginProvider)
      extends Processor[Custom]
      with StrictLogging {
    override def execute(process: Custom): Either[ProcessError, Dataset[Row]] = {
      logger.info(s"Executing custom process: $process")
      pluginProvider
        .load[ProcessorPlugin](process.clazz)
        .flatMap { processor =>
          try Right(processor.process(sparkSession, process.options))
          catch { case t: Throwable => Left(ProcessError(process, t)) }
        }
        .leftMap(ProcessError(process, _))
    }
  }

  class ProcessProcessor(sparkSession: SparkSession) extends Processor[Process] {
    override def execute(process: Process): Either[ProcessError, Dataset[Row]] =
      process match {
        case s: Sql    => new SqlProcessor(sparkSession).execute(s)
        case s: Custom => new CustomProcessor(sparkSession, new PluginProvider()).execute(s)
      }
  }

}
