import sbt._

object Dependencies {

  val LogbackV = "1.2.3"
  val CirceV = "0.11.0"
  val FinchV = "0.27.0"
  val DoobieV = "0.6.0"
  val PureConfigV = "0.10.1"
  val AkkaV = "2.5.19"

  val logback: Seq[ModuleID] = Seq(
    "ch.qos.logback" % "logback-classic" % LogbackV,
    "ch.qos.logback" % "logback-core" % LogbackV,
    "org.slf4j" % "jul-to-slf4j" % "1.7.25"
  )

  val circe: Seq[ModuleID] = Seq(
    "io.circe" %% "circe-generic" % CirceV,
    "io.circe" %% "circe-parser" % CirceV,
    "io.circe" %% "circe-generic-extras" % CirceV
  )

  val finch: Seq[ModuleID] = Seq(
    "com.github.finagle" %% "finchx-core" % FinchV,
    "com.github.finagle" %% "finchx-circe" % FinchV
  )

  val doobie: Seq[ModuleID] = Seq(
    "org.tpolecat" %% "doobie-core" % DoobieV,
    "org.tpolecat" %% "doobie-postgres" % DoobieV,
    "org.tpolecat" %% "doobie-h2" % DoobieV,
    "io.hydrosphere" %% "typed-sql" % "0.1.0"
  )

  val grpc: Seq[ModuleID] = Seq(
    "io.grpc" % "grpc-netty" % "1.18.0",
    "io.hydrosphere" %% "serving-grpc-scala" % "0.2.2"
  )

  val pureconfig: Seq[ModuleID] = Seq(
    "com.github.pureconfig" %% "pureconfig" % PureConfigV
  )
  
  val akka: Seq[ModuleID] = Seq(
    "com.typesafe.akka" %% "akka-actor-typed" % AkkaV,
    "com.typesafe.akka" %% "akka-slf4j" % AkkaV
  )

  val cats: Seq[ModuleID] = Seq(
    "org.typelevel" %% "cats-core" % "1.5.0",
    "org.typelevel" %% "cats-effect" % "1.1.0"
  )
  
  val math: Seq[ModuleID] = Seq(
    "org.scalanlp" %% "breeze" % "0.13.2"
  )

  val refined: Seq[ModuleID] = Seq(
    "eu.timepit" %% "refined" % "0.9.3"
  )

  val flyway: ModuleID = "org.flywaydb" % "flyway-core" % "5.2.1"
  
  val hashing: ModuleID = "net.openhft" % "zero-allocation-hashing" % "0.8"
  
  val enumeratum: ModuleID = "com.beachape" %% "enumeratum" % "1.5.13"
  
  val influx: ModuleID = "com.paulgoldbaum" %% "scala-influxdb-client" % "0.6.1"
  
  val mongo: ModuleID = "org.mongodb.scala" %% "mongo-scala-driver" % "2.5.0"
  
  val projectDeps: Seq[ModuleID] = 
    logback ++ circe ++ finch ++ doobie ++ grpc ++ pureconfig ++ akka ++ cats ++ math ++ refined :+ flyway :+ hashing :+ enumeratum :+ influx :+ mongo
}
