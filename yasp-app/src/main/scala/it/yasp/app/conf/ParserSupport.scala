package it.yasp.app.conf

import cats.implicits._
import io.circe.Decoder

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
    *   instance of type `A`
    */
  def parseYaml[A](content: String)(implicit ev: Decoder[A]): A =
    io.circe.yaml.parser
      .parse(content)
      .leftMap(err => err)
      .flatMap(_.as[A])
      .valueOr(throw _)
}
