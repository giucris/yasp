package it.yasp.app.conf

import cats.implicits._
import io.circe.Decoder

/** Provide a method to parse from yml content
  */
trait ParserSupport extends EncodersSupport {

  def parseYaml[A](content: String)(implicit ev: Decoder[A]): A =
    io.circe.yaml.parser
      .parse(content)
      .leftMap(err => err)
      .flatMap(_.as[A])
      .valueOr(throw _)
}
