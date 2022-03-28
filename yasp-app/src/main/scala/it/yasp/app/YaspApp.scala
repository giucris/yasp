package it.yasp.app

import it.yasp.app.args.YaspArgs
import it.yasp.app.conf.YaspExecutionLoader._
import it.yasp.service.YaspService

/** YaspApp
  *
  * An executable Yasp application. Process input args, that accept a yml file, parse the yml file,
  * create the relative YaspExecution and execute it.
  */
object YaspApp extends App {

  YaspArgs.parse(args).foreach { conf =>
    YaspService().run(load(conf.filePath))
  }

}
