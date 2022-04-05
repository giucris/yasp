package it.yasp.app.conf

import cats.implicits._
import io.circe.Decoder
import it.yasp.app.err.YaspAppErrors.ParseYmlError

/** Provide a method to parse a yml content
  */
trait ParserSupport extends DecodersSupport {

  /** Parse a yml content into a specific case class
    * @param content:
    *   String content
    * @param ev:
    *   Decoder[A]
    * @tparam A:
    *   case class instance to derivce
    * @return
    *   Right(A) otherwise Left(ParseYmlError)
    */
  def parseYaml[A](content: String)(implicit ev: Decoder[A]): Either[ParseYmlError, A] =
    io.circe.yaml.parser.parse(content).flatMap(_.as[A]) match {
      case Right(r) => Right(r)
      case Left(l)  => Left(ParseYmlError(content, l))
    }
}
