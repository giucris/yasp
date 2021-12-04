package it.yasp.core.spark.session

import it.yasp.core.spark.session.SessionType.{Distributed, Local}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** SparkSessionFactory provide a create method to build SparkSession starting from SparkSessionConf
  */
class SparkSessionFactory {

  private val LOCAL_MASTER = "local[*]"

  /** Crate a SparkSession
    * @param sessionConf:
    *   A [[SessionConf]] product type that describe how to build the SparkSession
    * @return
    *   A [[SparkSession]] created as described on the [[SessionConf]] provided as arguments.
    *
    * Given a 'SessionConf(Local,appName,conf)' create a SparkSession as follow:
    * {{{
    *   SparkSession
    *     .builder()
    *     .appName(appName)
    *     .config(new SparkConf().setAll(config))
    *     .master("local[*]")
    *     .getOrCreate()
    * }}}
    *
    * Given a SessionConf(Distributed, appName,config) create a SparkSession as follow:
    * {{{
    *   SparkSession
    *     .builder()
    *     .appName(appName)
    *     .config(new SparkConf().setAll(config))
    *     .getOrCreate()
    * }}}
    */
  def create(sessionConf: SessionConf): SparkSession =
    sessionConf match {
      case SessionConf(Local, an, c)       => builder(an, c).master(LOCAL_MASTER).getOrCreate()
      case SessionConf(Distributed, an, c) => builder(an, c).getOrCreate()
    }

  private def builder(appName: String, config: Map[String, String]) =
    SparkSession
      .builder()
      .appName(appName)
      .config(new SparkConf().setAll(config))

}
