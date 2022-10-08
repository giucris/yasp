package it.yasp.app.args

import it.yasp.app.err.YaspError.YaspArgsError
import scopt.OParser

/** YaspArgs model
  *
  * @param filePath:
  *   yasp yml file path
  * @param dryRun:
  *   yasp dry-run execution mode
  */
final case class YaspArgs(filePath: String = "", dryRun: Boolean = false)

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
        .required()
        .action((x, c) => c.copy(filePath = x))
        .text("yasp yml configuration file path"),
      builder
        .opt[Unit]("dry-run")
        .action((_, c) => c.copy(dryRun = true))
        .text(
          """
            |yasp dry-run flag that enable a dry-run execution.
            |A dry-run execution will execute all yasp process without running any spark action.
            |This is useful to test your configuration and to see how yasp will manage your steps
            |"""
        ),
      builder.help("help").text("Specify YaspExecution yml file path with -f option")
    )
    OParser.parse(parser, args, YaspArgs()).toRight(YaspArgsError(args))
  }

}
