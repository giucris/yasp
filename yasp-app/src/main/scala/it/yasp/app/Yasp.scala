package it.yasp.app

import it.yasp.app.args.YaspArgs._
import it.yasp.core.spark.model.Process.Sql
import it.yasp.core.spark.model.{Dest, Session, Source}
import it.yasp.core.spark.model.SessionType.Local
import it.yasp.service.YaspService
import it.yasp.service.model.{YaspExecution, YaspPlan, YaspProcess, YaspSink, YaspSource}

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
