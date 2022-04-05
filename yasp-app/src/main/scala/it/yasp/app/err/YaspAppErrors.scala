package it.yasp.app.err

/** YaspAppErrors Sum Type
  */
sealed trait YaspAppErrors extends Exception with Product with Serializable

object YaspAppErrors {

  /** ReadFileError model
    *
    * Represent an error occured during the file read phase
    * @param path:
    *   [[String]] file path
    * @param details:
    *   [[Throwable]] instance that cause this [[YaspAppErrors]]
    */
  final case class ReadFileError(
      path: String,
      details: Throwable
  ) extends YaspAppErrors

  /** InterpolationError model.
    *
    * Represent an error occured during the interpolation phase
    * @param yml:
    *   the yml content that yasp try to interpolate
    * @param details:
    *   a [[Throwable]] instance that cause this [[YaspAppErrors]]
    */
  final case class InterpolationError(yml: String, details: Throwable) extends YaspAppErrors

  /** ParseYmlError model
    *
    * Represent an error occured during the parsing phase
    * @param yml:
    *   the yml content on string format
    * @param details:
    *   the circe.Error that cause this [[YaspAppErrors]]
    */
  final case class ParseYmlError(
      yml: String,
      details: io.circe.Error
  ) extends YaspAppErrors

}
