package it.yasp.core.spark.registry

import org.apache.spark.sql.DataFrame

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
}

object Registry {

  /** DefaultRegistry will register the table as TempView
    */
  class DefaultRegistry extends Registry {
    override def register(df: DataFrame, name: String): Unit =
      df.createTempView(name)
  }
}
