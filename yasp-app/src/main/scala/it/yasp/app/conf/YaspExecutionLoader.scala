package it.yasp.app.conf

import io.circe.generic.auto._
import it.yasp.service.model.YaspExecution

object YaspExecutionLoader extends FileSupport with ParserSupport with VariablesSupport {

  def load(filePath: String): YaspExecution = {
    val content = interpolate(read(filePath), sys.env)
    parseYaml[YaspExecution](content)
  }

}
