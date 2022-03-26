package it.yasp.app.conf

import cats.implicits._
import io.circe.Decoder

trait ParserSupport {

  def parseYaml[A](content: String)(implicit ev: Decoder[A]): A =
    io.circe.yaml.parser
      .parse(content)
      .leftMap(err => err)
      .flatMap(_.as[A])
      .valueOr(throw _)
}
