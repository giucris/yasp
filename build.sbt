import sbt.url

lazy val commonSettings = Seq(
  organization := "it.yasp",
  licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.txt")),
  scalaVersion := Settings.SCALA_212,
  scalacOptions ++= Settings.yaspScalaCompilerSettings,
  developers   := List(
    Developer(
      id = "giucris",
      name = "Giuseppe Cristiano",
      email = "giucristiano89@gmail.com",
      url = url("https://github.com/giucris")
    )
  )
)

lazy val root = (project in file("."))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(Settings.yaspWartRemoverSettings)
  .aggregate(testKit, core, service, app)

lazy val testKit = (project in file("yasp-testkit"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(
    name := "yasp-testkit",
    Settings.yaspWartRemoverSettings,
    libraryDependencies ++= Seq(
      Dependencies.sparkSql,
      Dependencies.scalactic,
      Dependencies.scalaTest
    )
  )

lazy val core    = (project in file("yasp-core"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(
    name := "yasp-core",
    Settings.yaspWartRemoverSettings,
    libraryDependencies ++= Seq(
      Dependencies.catsCore,
      Dependencies.scalactic,
      Dependencies.typeSafeScalaLogging,
      Dependencies.logback,
      if (!Settings.yaspLightBuild) Dependencies.sparkSql else Dependencies.sparkSql         % Provided,
      if (!Settings.yaspLightBuild) Dependencies.sparkAvro else Dependencies.sparkAvro       % Provided,
      if (!Settings.yaspLightBuild) Dependencies.sparkXml else Dependencies.sparkXml         % Provided,
      if (!Settings.yaspLightBuild) Dependencies.sparkHive else Dependencies.sparkHive       % Provided,
      if (!Settings.yaspLightBuild) Dependencies.sparkDelta else Dependencies.sparkDelta     % Provided,
      if (!Settings.yaspLightBuild) Dependencies.sparkIceberg else Dependencies.sparkIceberg % Provided,
      Dependencies.scalaTest % Test,
      Dependencies.scalaMock % Test,
      Dependencies.h2db      % Test
    )
  )
  .dependsOn(testKit % Test)

lazy val service = (project in file("yasp-service"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(
    name := "yasp-service",
    Settings.yaspWartRemoverSettings,
    libraryDependencies ++= Seq(
      Dependencies.sparkSql  % Provided,
      Dependencies.scalaTest % Test,
      Dependencies.scalaMock % Test,
      Dependencies.h2db      % Test
    )
  )
  .dependsOn(core, testKit % Test)

lazy val app     = (project in file("yasp-app"))
  .settings(commonSettings)
  .settings(
    name := "yasp-app",
    Settings.yaspWartRemoverSettings,
    Settings.yaspAssemblySettings,
    libraryDependencies ++= Seq(
      Dependencies.scopt,
      Dependencies.apacheText,
      Dependencies.circeCore,
      Dependencies.circeGeneric,
      Dependencies.circeParser,
      Dependencies.circeParserYml,
      Dependencies.scalaTest % Test,
      Dependencies.scalaMock % Test,
      Dependencies.h2db      % Test
    )
  )
  .dependsOn(service, testKit % Test)
