package it.yasp.app

import it.yasp.app.args.YaspArgs

/** Yasp
  *
  * An executable Yasp application. Process input args, that accept a yml file location, and create
  * a YaspApp using the fromFile funcitons, create the relative YaspExecution and execute it.
  */
object Yasp extends App {

  YaspArgs.parse(args).foreach(conf => YaspApp.fromFile(conf.filePath))

}
