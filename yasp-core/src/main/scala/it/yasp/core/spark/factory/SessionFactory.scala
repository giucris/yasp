package it.yasp.core.spark.factory

import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.model.Session
import it.yasp.core.spark.model.SessionType.{Distributed, Local}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** SessionFactory
  *
  * Provide a create method to build SparkSession starting from Session model
  */
class SessionFactory extends StrictLogging {

  private val LOCAL_MASTER = "local[*]"

  /** Crate a SparkSession
    *
    * @param session
    *   : A [[Session]] product type that describe how to build the SparkSession
    * @return
    *   A [[SparkSession]] created as described on the [[Session]] provided as arguments.
    *
    * Given a 'Session(Local,appName,conf)' build a SparkSession as follow:
    * {{{
    *   SparkSession
    *     .builder()
    *     .appName(appName)
    *     .config(new SparkConf().setAll(config))
    *     .master("local[*]")
    *     .getOrCreate()
    * }}}
    *
    * Given a Session(Distributed, appName,config) create a SparkSession as follow:
    * {{{
    *   SparkSession
    *     .builder()
    *     .appName(appName)
    *     .config(new SparkConf().setAll(config))
    *     .getOrCreate()
    * }}}
    */
  def create(session: Session): SparkSession = {
    logger.info(s"Create SparkSession as: $session")
    session match {
      case Session(Local, name, c)       => builder(name, c).master(LOCAL_MASTER).getOrCreate()
      case Session(Distributed, name, c) => builder(name, c).getOrCreate()
    }
  }

  private def builder(appName: String, config: Map[String, String]): SparkSession.Builder =
    SparkSession
      .builder()
      .appName(appName)
      .config(new SparkConf().setAll(config))

}

object SessionFactory {

  def apply(): SessionFactory = new SessionFactory()
}
