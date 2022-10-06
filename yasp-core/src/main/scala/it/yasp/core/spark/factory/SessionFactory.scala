package it.yasp.core.spark.factory

import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.err.YaspCoreError.CreateSessionError
import it.yasp.core.spark.extensions.SparkExtensions.{SparkSessionBuilderOps, SparkSessionOps}
import it.yasp.core.spark.model.Session
import org.apache.spark.sql.SparkSession

/** SessionFactory
  *
  * Provide a create method to build SparkSession starting from Session model
  */
class SessionFactory extends StrictLogging {

  /** Crate a SparkSession
    *
    * @param session
    *   : A [[Session]] that define how to build a SparkSession
    * @return
    *   The [[SparkSession]] created as described.
    *
    * Given a 'Session(Local,appName,conf,None)' build a SparkSession as follow:
    * {{{
    *   SparkSession
    *     .builder()
    *     .appName(appName)
    *     .config(new SparkConf().setAll(conf))
    *     .master("local[*]")
    *     .getOrCreate()
    * }}}
    *
    * Given a Session(Distributed, appName,conf,None) create a SparkSession as follow:
    * {{{
    *   SparkSession
    *     .builder()
    *     .appName(appName)
    *     .config(new SparkConf().setAll(conf))
    *     .getOrCreate()
    * }}}
    *
    * Given a Session(_,_,_,checkPointDir) create a SparkSession as follow:
    * {{{
    *   val spark = SparkSession
    *     .builder()
    *     // other builder config
    *     .getOrCreate()
    *
    *   spark.sparkContext.setCheckpointDir(checkPointDir)
    * }}}
    */
  def create(session: Session): Either[CreateSessionError, SparkSession] = {
    logger.info(s"Creating SparkSession as: $session")
    try Right(
      SparkSession
        .builder()
        .appName(session.name)
        .withMaster(session.master)
        .withSparkConf(session.conf)
        .withHiveSupport(session.withHiveSupport)
        .getOrCreate()
        .withCheckPointDir(session.withCheckpointDir)
    )
    catch { case t: Throwable => Left(CreateSessionError(session, t)) }
  }

}

object SessionFactory {

  def apply(): SessionFactory = new SessionFactory()

}
