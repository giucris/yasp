package it.yasp.app

import it.yasp.app.args.YaspArgs
import it.yasp.app.conf.YaspExecutionLoader._
import it.yasp.service.YaspService

object YaspApp extends App {

  YaspArgs.parse(args).foreach { conf =>
    YaspService().run(load(conf.filePath))
  }

}
