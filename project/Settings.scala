import sbt.Compile
import sbt.Keys._
import sbtassembly.AssemblyKeys.{assemblyCacheOutput, assemblyJarName}
import sbtassembly.AssemblyPlugin.autoImport.{assembly, assemblyMergeStrategy, MergeStrategy}
import sbtassembly.PathList
import wartremover.WartRemover.autoImport.{wartremoverErrors, Wart, Warts}

object Settings {

  lazy val scalaCompilerOptions = Seq(
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

  lazy val wartRemover = Seq(
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

  def appAssembly(sparkVersion: String) = Seq(
    assembly / mainClass             := Some("it.yasp.app.Yasp"),
    assembly / assemblyJarName       := s"${name.value}-$sparkVersion-${version.value}.jar",
    assembly / assemblyCacheOutput   := false,
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _ @_*) => MergeStrategy.discard
      case _                           => MergeStrategy.first
    }
  )
}
