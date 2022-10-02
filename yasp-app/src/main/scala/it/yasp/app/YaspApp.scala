package it.yasp.app

import com.typesafe.scalalogging.StrictLogging
import io.circe.generic.auto._
import it.yasp.app.err.YaspError
import it.yasp.app.err.YaspError.YaspExecutionError
import it.yasp.app.support.{FileSupport, ParserSupport, VariablesSupport}
import it.yasp.service.YaspService
import it.yasp.service.model.YaspExecution

object YaspApp extends FileSupport with ParserSupport with VariablesSupport with StrictLogging {

  /** Load a YaspExecution from a file.
    *
    * Read the file content and then call the [[fromYaml(content)]] with the file content readed.
    *
    * @param filePath
    *   : a yml file path
    * @return
    *   Unit
    */
  def fromFile(filePath: String): Either[YaspError, Unit] =
    for {
      content <- read(filePath)
      _       <- fromYaml(content)
    } yield ()

  /** Load a YaspExecution from a yml content.
    *
    * Interpolate the yaml content with environment variable, then parse the yaml in a
    * [[YaspExecution]] and call the [[YaspService.run]] method to start the ETL job.
    *
    * @param content:
    *   YaspExecution in yml format
    * @return
    */
  def fromYaml(content: String): Either[YaspError, Unit] = {
    logger.info(s"Initialize Yasp Application from yaml content:\n$content")
    for {
      contentWithEnv <- interpolate(content, sys.env)
      yaspExecution  <- parseYaml[YaspExecution](contentWithEnv)
      _              <- exec(yaspExecution)
      _ = logger.info(s"Yasp Application completed successful.")
    } yield ()
  }

  private def exec(yaspExecution: YaspExecution): Either[YaspExecutionError, Unit] =
    try Right(YaspService().run(yaspExecution))
    catch { case t: Throwable => Left(YaspExecutionError(yaspExecution, t)) }
}
