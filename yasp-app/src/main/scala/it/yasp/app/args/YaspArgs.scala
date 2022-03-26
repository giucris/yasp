package it.yasp.app.args

import scopt.OParser

case class YaspArgs(filePath: String = "")

object YaspArgs {

  def parse(args: Array[String]): Option[YaspArgs] = {
    val builder = OParser.builder[YaspArgs]
    val parser  =
      OParser.sequence(
        builder.programName("Yasp"),
        builder.head("Yasp", "0.0.1"),
        builder
          .opt[String]('f', "file")
          .action((x, c) => c.copy(filePath = x))
          .text("yasp yml configuration file path"),
        builder.help("help").text("Specify YaspExecution yml file path with -f option")
      )
    OParser.parse(parser, args, YaspArgs())
  }

}
