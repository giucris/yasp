package it.yasp.app

import it.yasp.app.args.YaspArgs._

/** Yasp
  *
  * An executable Yasp application. Process input args, that accept a yml file location, and create
  * a YaspApp using the fromFile funcitons, create the relative YaspExecution and execute it.
  */
object Yasp {

  private val banner =
    """
      | _     _
      || |   | |
      || |___| | _____   ___  ____
      ||_____  |(____ | /___)|  _ \
      | _____| |/ ___ ||___ || |_| |
      |(_______|\_____|(___/ |  __/
      |                      |_|
      |Yet Another SPark framework
      |""".stripMargin

  def main(args: Array[String]): Unit = {
    println(banner)
    parse(args).map(c => YaspApp.fromFile(c.filePath)) match {
      case Some(Right(_)) => sys.exit()
      case Some(Left(_))  => sys.exit(1)
      case None           => sys.exit(2)
    }
  }

}
