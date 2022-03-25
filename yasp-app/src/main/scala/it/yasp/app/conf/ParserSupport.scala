package it.yasp.app.conf

import io.circe.Decoder
import cats.implicits._

trait ParserSupport {

  def parseYaml[A](content: String)(implicit ev: Decoder[A]): A =
    io.circe.yaml.parser
      .parse(content)
      .leftMap(err => err)
      .flatMap(_.as[A])
      .valueOr(throw _)
}
