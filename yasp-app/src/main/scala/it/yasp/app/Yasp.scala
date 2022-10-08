package it.yasp.app

import com.typesafe.scalalogging.StrictLogging
import it.yasp.app.args.YaspArgs._

/** Yasp
  *
  * An executable Yasp application. Process input args, that accept a yml file location, and create
  * a YaspApp using the fromFile funcitons, create the relative YaspExecution and execute it.
  */
object Yasp extends StrictLogging {

  private val banner =
    """
      |******************************************
      |******************************************
      |******************************************
      |***     _     _                        ***
      |***    | |   | |                       ***
      |***    | |___| | _____   ___  ____     ***
      |***    |_____  |(____ | /___)|  _ \    ***
      |***     _____| |/ ___ ||___ || |_| |   ***
      |***    (_______|\_____|(___/ |  __/    ***
      |***                          |_|       ***
      |******************************************
      |******* Yet Another SPark framework ******
      |******************************************
      |
      |""".stripMargin

  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  def main(args: Array[String]): Unit = {
    println(banner)
    parse(args)
      .flatMap(YaspApp.fromArgs)
      .fold(
        error => {
          logger.error("Yasp Error: ", error)
          throw error
        },
        _ => logger.info("Yasp Success.")
      )
  }

}
