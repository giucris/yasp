package it.yasp.app

import it.yasp.app.args.YaspArgs
import it.yasp.app.conf.{ParserSupport, VariablesSupport, YaspExecutionLoader}
import it.yasp.core.spark.session.SparkSessionFactory
import it.yasp.service.YaspExecutor.DefaultYaspExecutor
import it.yasp.service.YaspServiceFactory

object YaspApp extends App with ParserSupport with VariablesSupport {

  YaspArgs.parse(args).foreach { args =>
    new DefaultYaspExecutor(
      new SparkSessionFactory,
      new YaspServiceFactory
    ).exec(YaspExecutionLoader.load(args.filePath))
  }

}
