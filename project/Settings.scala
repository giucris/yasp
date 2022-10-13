import sbt.Compile
import sbt.Keys._
import sbtassembly.AssemblyKeys.{assemblyCacheOutput, assemblyJarName}
import sbtassembly.AssemblyPlugin.autoImport.{assembly, assemblyMergeStrategy, MergeStrategy}
import sbtassembly.PathList
import wartremover.WartRemover.autoImport.{wartremoverErrors, Wart, Warts}

object Settings {
  lazy val SCALA_212 = "2.12.15"
  lazy val SPARK_330 = "3.3.0"
  lazy val LIGHT_BUILD = "LIGHT"

  //Default spark version set to 3.3.0 if not provided
  lazy val yaspSparkVersion: String =
    sys.props.getOrElse("yasp.spark.version", SPARK_330)

  lazy val yaspLightBuild: Boolean =
    sys.props.get("yasp.build.version").contains(LIGHT_BUILD)

  lazy val yaspScalaCompilerSettings = Seq(
    "-deprecation",
    "-feature",
    "-unchecked",
    "-Xlint:_,-missing-interpolator",
    "-encoding",
    "UTF-8",
    "-Xfatal-warnings",
    "-Ypartial-unification",
    "-Ywarn-dead-code",
    "-Ywarn-inaccessible",
    "-Ywarn-unused-import",
    "-Ywarn-infer-any",
    "-target:jvm-1.8",
    "-language:implicitConversions",
    "-language:higherKinds",
    "-language:existentials",
    "-language:postfixOps"
  )

  lazy val yaspWartRemoverSettings = Seq(
    Compile / compile / wartremoverErrors ++= Warts.allBut(
      Wart.Nothing,
      Wart.DefaultArguments,
      Wart.NonUnitStatements,
      Wart.Equals,
      Wart.Option2Iterable,
      Wart.TraversableOps,
      Wart.Any
    )
  )

  lazy val yaspAssemblySettings = Seq(
    assembly / mainClass             := Some("it.yasp.app.Yasp"),
    assembly / assemblyJarName       := s"${name.value}-spark-$yaspSparkVersion-${version.value}.jar",
    assembly / assemblyCacheOutput   := false,
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _ @_*) => MergeStrategy.discard
      case _                           => MergeStrategy.first
    }
  )
}
