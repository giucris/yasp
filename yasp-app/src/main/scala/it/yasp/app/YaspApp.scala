package it.yasp.app

import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import io.circe.generic.auto._
import it.yasp.app.args.YaspArgs
import it.yasp.app.err.YaspError
import it.yasp.app.err.YaspError.YaspExecutionError
import it.yasp.app.support.{FileSupport, ParserSupport, VariablesSupport}
import it.yasp.service.YaspService
import it.yasp.service.err.YaspServiceError
import it.yasp.service.model.YaspExecution

object YaspApp extends FileSupport with ParserSupport with VariablesSupport with StrictLogging {

  /** Load a YaspExecution from input args.
    *
    * @param yaspArgs:
    *   An instance of [[YaspArgs]]
    * @return
    *   Right(Unit) if process completed successful Left(YaspError) otherwise
    */
  def fromArgs(yaspArgs: YaspArgs): Either[YaspError, Unit] =
    fromFile(yaspArgs.filePath, yaspArgs.dryRun)

  /** Load a YaspExecution from a file.
    *
    * Read the file content and then call the [[fromYaml(content)]] with the file content readed.
    *
    * @param filePath
    *   : a yml file path
    * @return
    *   Unit
    */
  def fromFile(filePath: String, dryRun: Boolean): Either[YaspError, Unit] =
    for {
      content <- read(filePath)
      _       <- fromYaml(content, dryRun)
    } yield ()

  /** Load a YaspExecution from a yml content.
    *
    * Interpolate the yaml content with environment variable, then parse the yaml in a [[YaspExecution]] and call the
    * [[YaspService.run]] method to start the ETL job.
    *
    * @param content:
    *   YaspExecution in yml format
    * @return
    */
  def fromYaml(content: String, dryRun: Boolean): Either[YaspError, Unit] = {
    logger.info(s"Initialize Yasp Application from yaml content:\n$content")
    for {
      contentWithEnv <- interpolate(content, sys.env)
      yaspExecution  <- parseYaml[YaspExecution](contentWithEnv)
      _              <- run(yaspExecution, dryRun).leftMap(e => YaspExecutionError(yaspExecution, e))
      _ = logger.info(s"Yasp Application completed successful.")
    } yield ()
  }

  private def run(yaspExecution: YaspExecution, dryRun: Boolean): Either[YaspServiceError, Unit] =
    if (dryRun) Right(())
    else YaspService().run(yaspExecution)
}
