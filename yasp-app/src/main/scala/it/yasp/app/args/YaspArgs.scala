package it.yasp.app.args

import it.yasp.app.err.YaspError.YaspArgsError
import scopt.OParser

/** YaspArgs model
  *
  * @param filePath:
  *   the yasp file path
  */
final case class YaspArgs(filePath: String = "")

object YaspArgs {

  /** Parse YaspArgs from args Array
    * @param args:
    *   [[Array]] of String
    * @return
    *   Some(YaspArgs) otherwise None
    */
  def parse(args: Array[String]): Either[YaspArgsError, YaspArgs] = {
    val builder = OParser.builder[YaspArgs]
    val parser  = OParser.sequence(
      builder.programName("Yasp"),
      builder.head("Yasp", "0.0.1"),
      builder
        .opt[String]('f', "file")
        .action((x, c) => c.copy(filePath = x))
        .text("yasp yml configuration file path"),
      builder.help("help").text("Specify YaspExecution yml file path with -f option")
    )
    OParser.parse(parser, args, YaspArgs()).toRight(YaspArgsError(args))
  }

}
