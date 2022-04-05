package it.yasp.app.support

import it.yasp.app.err.YaspAppErrors.ReadFileError

import scala.io.Source

/** FileSupport trait
  *
  * Provide a method to read a file content
  */
trait FileSupport {

  /** Read a file on the provided path
    * @param filePath:
    *   file path [[String]] location
    * @return
    *   Right(String) otherwise Left(ReadFileError)
    */
  def read(filePath: String): Either[ReadFileError, String] =
    try {
      val source = Source.fromFile(filePath, "UTF-8")
      try Right(source.mkString.trim)
      catch { case t: Throwable => Left(ReadFileError(filePath, t)) }
      finally source.close()
    } catch { case t: Throwable => Left(ReadFileError(filePath, t)) }

}
