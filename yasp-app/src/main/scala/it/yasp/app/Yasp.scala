package it.yasp.app

import it.yasp.app.args.YaspArgs._

/** Yasp
  *
  * An executable Yasp application. Process input args, that accept a yml file location, and create
  * a YaspApp using the fromFile funcitons, create the relative YaspExecution and execute it.
  */
object Yasp {

  def main(args: Array[String]): Unit =
    parse(args).map(c => YaspApp.fromFile(c.filePath)) match {
      case Some(Right(_)) => sys.exit()
      case Some(Left(_))  => sys.exit(1)
      case None           => sys.exit(2)
    }

}
