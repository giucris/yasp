package it.yasp.app

import it.yasp.app.args.YaspArgs._

/** Yasp
  *
  * An executable Yasp application. Process input args, that accept a yml file location, and create
  * a YaspApp using the fromFile funcitons, create the relative YaspExecution and execute it.
  */
object Yasp {

  def main(args: Array[String]): Unit =
    parse(args).foreach(c => YaspApp.fromFile(c.filePath))

}
