package it.yasp.app.support

import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import io.circe.Decoder
import it.yasp.app.err.YaspAppErrors.ParseYmlError

/** Provide a method to parse a yml content
  */
trait ParserSupport extends DecodersSupport with StrictLogging {

  /** Parse a yml content into a specific case class
    * @param content:
    *   String content
    * @param ev:
    *   Decoder[A]
    * @tparam A:
    *   case class instance to derive
    * @return
    *   Right(A) otherwise Left(ParseYmlError)
    */
  def parseYaml[A](content: String)(implicit ev: Decoder[A]): Either[ParseYmlError, A] = {
    logger.info(s"Parse yaml content")
    io.circe.yaml.parser
      .parse(content)
      .flatMap(_.as[A])
      .leftMap(e => ParseYmlError(content, e))
  }
}
