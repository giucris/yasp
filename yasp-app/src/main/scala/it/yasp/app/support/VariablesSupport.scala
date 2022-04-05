package it.yasp.app.support

import it.yasp.app.err.YaspAppErrors.InterpolationError
import org.apache.commons.text.StringSubstitutor

import scala.collection.JavaConverters.mapAsJavaMapConverter

/** VariablesSupport trait
  *
  * Provide a method to handle variable
  */
trait VariablesSupport {

  /** Interpolate system environment variable with a provided value
    * @param value:
    *   [[String]] value
    * @param values:
    *   [[Map]] of String that will be used as look up for the interpolation process
    * @return
    *   Right(values) otherwise Left(InterpolationError)
    */
  def interpolate(value: String, values: Map[String, String]): Either[InterpolationError, String] =
    try {
      val subs = new StringSubstitutor(values.asJava).setEnableUndefinedVariableException(true)
      Right(subs.replace(value))
    } catch { case t: Throwable => Left(InterpolationError(value, t)) }
}
