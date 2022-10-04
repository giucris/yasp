package it.yasp.core.spark.registry

import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.err.YaspCoreError.{RegisterTableError, RetrieveTableError}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/** Registry trait
  */
trait Registry {

  /** Register a dataset with the provided name
    * @param dataset:
    *   Dataset
    * @param name:
    *   DataFrame name
    */
  def register(dataset: Dataset[Row], name: String): Either[RegisterTableError, Unit]

  /** Retrieve a dataset
    * @param name:
    *   Dataset name
    * @return
    *   a Dataset
    */
  def retrieve(name: String): Either[RetrieveTableError, Dataset[Row]]
}

object Registry {

  /** DefaultRegistry will register the table as TempView
    */
  class DefaultRegistry(spark: SparkSession) extends Registry with StrictLogging {

    override def register(dataset: Dataset[Row], name: String): Either[RegisterTableError, Unit] = {
      logger.info(s"Registering dataset as temp view with table name: $name")
      try {
        dataset.createTempView(name)
        Right(())
      } catch { case t: Throwable => Left(RegisterTableError(name, t)) }
    }

    override def retrieve(name: String): Either[RetrieveTableError, Dataset[Row]] = {
      logger.info(s"Retrieving table as dataset with table name: $name")
      try Right(spark.table(name))
      catch { case t: Throwable => Left(RetrieveTableError(name, t)) }
    }
  }
}
