import sbt.Compile
import sbt.Keys.compile
import wartremover.WartRemover.autoImport.{wartremoverErrors, Wart, Warts}

object Settings {

  lazy val scalaCompilerSettings = Seq(
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

  lazy val wartRemoverSettings = Seq(
    wartremoverErrors in (Compile, compile) ++= Warts.allBut(
      Wart.Nothing,
      Wart.DefaultArguments,
      Wart.NonUnitStatements,
      Wart.Equals,
      Wart.Option2Iterable,
      Wart.TraversableOps,
      Wart.Any
    )
  )

}
