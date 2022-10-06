package it.yasp.core.spark.extensions

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkExtensions {

  /** SparkSessionBuilderOps
    * @param builder:
    *   SparkSession.Builder
    */
  implicit class SparkSessionBuilderOps(builder: SparkSession.Builder) extends StrictLogging {

    /** Optionally set the Spark master
      * @param master:
      *   Optional string value of the spark master
      * @return
      *   if master isDefined return [[SparkSession.Builder]] with master configured otherwise
      *   return [[SparkSession.Builder]] without any conf
      */
    def withMaster(master: Option[String]): SparkSession.Builder =
      master.fold(builder) { c =>
        logger.info(s"Configuring master to: $c")
        builder.master(c)
      }

    /** Optionally set the Spark configuration provided
      * @param conf:
      *   Optional Map[String,String]
      * @return
      *   if conf isDefined return [[SparkSession.Builder]] otherwise return
      *   [[SparkSession.Builder]] without any conf
      */
    def withSparkConf(conf: Option[Map[String, String]]): SparkSession.Builder =
      conf.filter(_.nonEmpty).fold(builder) { c =>
        logger.info(s"Configuring SparkConf on SparkSessionBuilder with config: $conf")
        builder.config(new SparkConf().setAll(c))
      }

    /** Optionally enable the hive support
      * @param hiveSupport:
      *   Optional boolean
      * @return
      *   if hiveSupport isDefined and is True return [[SparkSession.Builder]] otherwise return
      *   [[SparkSession.Builder]] without any conf
      */
    def withHiveSupport(hiveSupport: Option[Boolean]): SparkSession.Builder =
      hiveSupport.filter(identity).fold(builder) { _ =>
        logger.info(s"Enabling SparkSessionBuilder HiveSupport")
        builder.enableHiveSupport()
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
      *   if checkPointDir isDefined [[SparkSession]] otherwise return [[SparkSession]] without any
      *   conf
      */
    def withCheckPointDir(checkPointDir: Option[String]): SparkSession =
      checkPointDir.fold(sparkSession) { c =>
        logger.info(s"Configuring checkPointDir on SparkSession as: $checkPointDir")
        sparkSession.sparkContext.setCheckpointDir(c)
        sparkSession
      }
  }

}
