package it.yasp.core.spark.factory

import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.err.YaspCoreError.CreateSessionError
import it.yasp.core.spark.factory.SessionFactory.{SparkSessionBuilderOps, SparkSessionOps}
import it.yasp.core.spark.model.IcebergCatalog.{HadoopIcebergCatalog, HiveIcebergCatalog}
import it.yasp.core.spark.model.{IcebergCatalog, Session}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.SPARK_SESSION_EXTENSIONS

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
        .maybeWithMaster(session.master)
        .maybeWithSparkConf(session.conf)
        .maybeWithHiveSupport(session.withHiveSupport)
        .maybeWithDeltaSupport(session.withDeltaSupport)
        .getOrCreate()
        .maybeWithCheckPointDir(session.withCheckpointDir)
    )
    catch { case t: Throwable => Left(CreateSessionError(session, t)) }
  }

}

object SessionFactory {

  def apply(): SessionFactory = new SessionFactory()

  /** SparkSessionBuilderOps
    * @param builder:
    *   [[SparkSession.Builder]]
    */
  implicit class SparkSessionBuilderOps(builder: SparkSession.Builder) extends StrictLogging {
    private val SPARK_CATALOG = "org.apache.iceberg.spark.SparkCatalog"
    private val DELTA_EXTENSION   = "io.delta.sql.DeltaSparkSessionExtension"
    private val DELTA_CATALOG     = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    private val ICEBERG_EXTENSION = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    private val ICEBERG_CATALOG   = "org.apache.iceberg.spark.SparkSessionCatalog"

    /** Optionally set the Spark master
      * @param master:
      *   Optional string value of the spark master
      * @return
      *   if master isDefined return [[SparkSession.Builder]] with master configured otherwise return
      *   [[SparkSession.Builder]] without any conf
      */
    def maybeWithMaster(master: Option[String]): SparkSession.Builder =
      master.fold(builder) { c =>
        logger.info(s"Configuring master to: $c")
        builder.master(c)
      }

    /** Optionally set the Spark configuration provided
      * @param conf:
      *   Optional Map[String,String]
      * @return
      *   if conf isDefined return [[SparkSession.Builder]] otherwise return [[SparkSession.Builder]] without any conf
      */
    def maybeWithSparkConf(conf: Option[Map[String, String]]): SparkSession.Builder =
      conf.filter(_.nonEmpty).fold(builder) { c =>
        logger.info(s"Configuring SparkConf on SparkSessionBuilder with config: $conf")
        builder.config(new SparkConf().setAll(c))
      }

    /** Optionally enable the hive support
      * @param hiveSupport:
      *   Optional boolean
      * @return
      *   if hiveSupport isDefined and is True return [[SparkSession.Builder]] otherwise return [[SparkSession.Builder]]
      *   without any conf
      */
    def maybeWithHiveSupport(hiveSupport: Option[Boolean]): SparkSession.Builder =
      hiveSupport.filter(identity).fold(builder) { _ =>
        logger.info(s"Enabling SparkSessionBuilder HiveSupport")
        builder.enableHiveSupport()
      }

    /** Optionally enable Delta support adding standard delta configuration on SparkConf
      * @param deltaSupport:
      *   Optional boolean
      * @return
      *   if deltaSupport is Defined and is True return [[SparkSession.Builder]] otherwise return
      *   [[SparkSession.Builder]] without any conf
      */
    def maybeWithDeltaSupport(deltaSupport: Option[Boolean]): SparkSession.Builder =
      deltaSupport.filter(identity).fold(builder) { _ =>
        logger.info(s"Updating SparkConf with Delta config")
        builder
          .config(SPARK_SESSION_EXTENSIONS.key, DELTA_EXTENSION)
          .config("spark.sql.catalog.spark_catalog", DELTA_CATALOG)
      }

    def maybeWithIcebergSupport(icebergSupport: Option[Boolean]): SparkSession.Builder =
      icebergSupport.filter(identity).fold(builder) { _ =>
        builder
          .config(SPARK_SESSION_EXTENSIONS.key, ICEBERG_EXTENSION)
          .config("spark.sql.catalog.spark_catalog", ICEBERG_CATALOG)
      }

  }

  /** SparkSessionOps
    * @param sparkSession:
    *   [[SparkSession]]
    */
  implicit class SparkSessionOps(sparkSession: SparkSession) extends StrictLogging {

    /** Optionally set the checkPointDir
      * @param checkPointDir:
      *   Optional checkpoint dir
      * @return
      *   if checkPointDir isDefined [[SparkSession]] otherwise return [[SparkSession]] without any conf
      */
    def maybeWithCheckPointDir(checkPointDir: Option[String]): SparkSession =
      checkPointDir.fold(sparkSession) { c =>
        logger.info(s"Configuring checkPointDir on SparkSession as: $checkPointDir")
        sparkSession.sparkContext.setCheckpointDir(c)
        sparkSession
      }
  }

}
