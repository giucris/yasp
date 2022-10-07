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

  lazy val appAssembly = Seq(
    assembly / mainClass             := Some("it.yasp.app.Yasp"),
    assembly / assemblyJarName       := s"${name.value}-${version.value}.jar",
    assembly / assemblyCacheOutput   := false,
    assembly / assemblyMergeStrategy := {
      case "module-info.class"                                          => MergeStrategy.first
      case "plugin.xml"                                                 => MergeStrategy.first
      case "git.properties"                                             => MergeStrategy.first
      case PathList("META-INF", "io.netty.versions.properties")         => MergeStrategy.first
      case PathList("META-INF", "versions", _ @_*)                      => MergeStrategy.first
      case PathList("META-INF", "org", "apache", "logging", _ @_*)      => MergeStrategy.first
      case PathList("javax", "annotation", _ @_*)                       => MergeStrategy.first
      case PathList("javax", "jdo", _ @_*)                              => MergeStrategy.first
      case PathList("org", "apache", "commons", _ @_*)                  => MergeStrategy.first
      case PathList("org", "apache", "spark", _ @_*)                    => MergeStrategy.first
      case PathList("org", "apache", "hadoop", "hive", "common", _ @_*) => MergeStrategy.first
      case x                                                            =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )
}
