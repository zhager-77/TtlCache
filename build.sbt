scalaVersion := "3.9.0"

lazy val root = rootProject
  .settings(
    name := "TtlCache",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "3.5.4"
    )
  )