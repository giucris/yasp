package it.yasp.core.spark.factory

import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.err.YaspCoreError.CreateSessionError
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
  def create(session: Session): Either[CreateSessionError, SparkSession] = {
    logger.info(s"Creating SparkSession as: $session")
    try session match {
      case Session(Local, name, c)       => Right(builder(name, c).master(LOCAL_MASTER).getOrCreate())
      case Session(Distributed, name, c) => Right(builder(name, c).getOrCreate())
    } catch { case t: Throwable => Left(CreateSessionError(session, t)) }
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
