package it.yasp.parser

import cats.syntax.either._
import it.yasp.model.YaspExecution

class YaspParser() {
  def parse(content: String): YaspExecution = {
    import io.circe.generic.auto._
    io.circe.yaml.parser
      .parse(content)
      .leftMap(err => err)
      .flatMap(_.as[YaspExecution])
      .valueOr(throw _)
  }
}
