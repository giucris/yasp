package it.yasp.args

case class YaspArgs(filePath: String = "")

object YaspArgs {

  def parse(args: Array[String]): Option[YaspArgs] = {
    import scopt.OParser
    val builder = OParser.builder[YaspArgs]
    val parser = {
      import builder._
      OParser.sequence(
        programName("Yasp"),
        head("Yasp", "0.0.1"),
        opt[String]('f', "file")
          .action((x, c) => c.copy(filePath = x))
          .text("yasp yml configuration file path")
      )
    }
    OParser.parse(parser, args, YaspArgs())
  }

}
