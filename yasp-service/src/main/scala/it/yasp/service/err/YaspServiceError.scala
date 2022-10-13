package it.yasp.service.err

import it.yasp.core.spark.err.YaspCoreError
import it.yasp.service.model.{YaspProcess, YaspSink, YaspSource}

/** YaspCoreError Sum Type
  */
sealed abstract class YaspServiceError(val message: String, val cause: Throwable)
    extends Exception(message, cause)
    with Product
    with Serializable

object YaspServiceError {

  final case class YaspInitError(
      yaspCoreError: YaspCoreError
  ) extends YaspServiceError(
        message = s"Unable to initialize yasp service",
        cause = yaspCoreError
      )

  final case class YaspLoaderError(
      source: YaspSource,
      yaspCoreError: YaspCoreError
  ) extends YaspServiceError(
        message = s"Unable to load source: $source",
        cause = yaspCoreError
      )

  final case class YaspProcessError(
      process: YaspProcess,
      yaspCoreError: YaspCoreError
  ) extends YaspServiceError(
        message = s"Unable to execute process: $YaspProcess",
        cause = yaspCoreError
      )

  final case class YaspWriterError(
      sink: YaspSink,
      yaspCoreError: YaspCoreError
  ) extends YaspServiceError(
        message = s"Unable to write sink: $sink",
        cause = yaspCoreError
      )
}
