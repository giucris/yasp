package it.yasp.core.spark.factory

import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.err.YaspCoreError.CreateSessionError
import it.yasp.core.spark.model.Session
import org.apache.spark.SparkConf
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
    try Right {
      createSession(
        sessionBuilder(session.name, session.conf),
        session.master,
        session.checkPointLocation
      )
    } catch { case t: Throwable => Left(CreateSessionError(session, t)) }
  }

  private def sessionBuilder(appName: String, config: Map[String, String]): SparkSession.Builder =
    SparkSession
      .builder()
      .appName(appName)
      .config(new SparkConf().setAll(config))

  private def createSession(
      sessionBuilder: SparkSession.Builder,
      master: Option[String],
      checkPointDir: Option[String]
  ): SparkSession = {
    val session = master.fold(sessionBuilder)(sessionBuilder.master).getOrCreate()
    checkPointDir.fold(session)(setCheckPointDir(session, _))
  }

  private def setCheckPointDir(sparkSession: SparkSession, checkPointDir: String): SparkSession = {
    logger.info(s"Configuring checkpoint directory to: $checkPointDir")
    sparkSession.sparkContext.setCheckpointDir(checkPointDir)
    sparkSession
  }

}

object SessionFactory {

  def apply(): SessionFactory = new SessionFactory()
}
