package it.yasp.app.err

import it.yasp.service.model.YaspExecution

/** YaspAppErrors Sum Type
  */
sealed abstract class YaspError(val message: String, val cause: Throwable)
    extends Exception(message, cause)
    with Product
    with Serializable

object YaspError {

  final case class YaspArgsError(
      args: Seq[String]
  ) extends YaspError(s"Unable to pars args", new IllegalArgumentException(args.mkString(",")))

  /** ReadFileError model
    *
    * Represent an error happen during the file read phase
    *
    * @param filePath
    *   : [[String]] file path
    * @param throwable
    *   : [[Throwable]] instance that cause this [[YaspError]]
    */
  final case class ReadFileError(
      filePath: String,
      throwable: Throwable
  ) extends YaspError(s"Unable to read file at:$filePath", throwable)

  /** InterpolationError model.
    *
    * Represent an error happen during the interpolation phase
    *
    * @param yaml
    *   : the yml content that yasp try to interpolate
    * @param throwable
    *   : a [[Throwable]] instance that cause this [[YaspError]]
    */
  final case class InterpolationError(
      yaml: String,
      throwable: Throwable
  ) extends YaspError(s"Unable to interpolate variable on yaml content: $yaml", throwable)

  /** ParseYmlError model
    *
    * Represent an error happen during the parsing phase
    *
    * @param yaml
    *   : the yml content on string format
    * @param throwable
    *   : the circe.Error that cause this [[YaspError]]
    */
  final case class ParseYmlError(
      yaml: String,
      throwable: io.circe.Error
  ) extends YaspError(s"Unable to parse yaml content: $yaml", throwable)

  /** YaspExecutionError model
    *
    * Represent an error happen during the processing of the plan provided.
    * @param yaspExecution:
    *   [[YaspExecution]] instance provided
    * @param throwable:
    *   a [[Throwable]] instance
    */
  final case class YaspExecutionError(
      yaspExecution: YaspExecution,
      throwable: Throwable
  ) extends YaspError(s"Unable to execute the provided execution: $yaspExecution", throwable)
}
