package it.yasp.core.spark.registry

import com.typesafe.scalalogging.StrictLogging
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
  def register(dataset: Dataset[Row], name: String): Unit

  /** Retrieve a dataset
    * @param name:
    *   Dataset name
    * @return
    *   a Dataset
    */
  def retrieve(name: String): Dataset[Row]
}

object Registry {

  /** DefaultRegistry will register the table as TempView
    */
  class DefaultRegistry(spark: SparkSession) extends Registry with StrictLogging {

    override def register(dataset: Dataset[Row], name: String): Unit = {
      logger.info(s"Register data: $name")
      dataset.createTempView(name)
    }

    override def retrieve(name: String): Dataset[Row] = {
      logger.info(s"Retrieve data: $name")
      spark.table(name)
    }
  }
}
