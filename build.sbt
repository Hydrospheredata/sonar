import sbt._

name := "sonar"

scalaVersion := "2.12.7"

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-Ypartial-unification"
)

resolvers += "Flyway" at "https://flywaydb.org/repo"

libraryDependencies ++= Dependencies.projectDeps

cancelable in Global := true
