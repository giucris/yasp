package it.yasp.core.spark.registry

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Registry trait
  */
trait Registry {

  /** Register a dataframe with the provided name
    * @param df:
    *   DataFrame
    * @param name:
    *   DataFrame name
    */
  def register(df: DataFrame, name: String): Unit

  /** Retrieve a dataframe
    * @param name:
    *   DataFrame name
    * @return
    *   a DataFrame
    */
  def retrieve(name: String): DataFrame
}

object Registry {

  /** DefaultRegistry will register the table as TempView
    */
  class DefaultRegistry(spark: SparkSession) extends Registry {

    override def register(df: DataFrame, name: String): Unit =
      df.createTempView(name)

    override def retrieve(name: String): DataFrame =
      spark.table(name)
  }
}
