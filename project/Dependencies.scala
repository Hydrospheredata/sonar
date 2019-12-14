import sbt._

object Dependencies {

  val LogbackV = "1.2.3"
  val CirceV = "0.11.0"
  val FinchV = "0.31.0"
  val DoobieV = "0.6.0"
  val PureConfigV = "0.10.1"
  val AkkaV = "2.5.19"
  val HadoopV = "3.2.1"
  val ParquetV = "1.11.0"

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
    "com.github.finagle" %% "finchx-circe" % FinchV,
    "com.github.finagle" %% "finchx-fs2" % FinchV,
    "com.github.finagle" %% "finchx-iteratee" % FinchV
  )

  val doobie: Seq[ModuleID] = Seq(
    "org.tpolecat" %% "doobie-core" % DoobieV,
    "org.tpolecat" %% "doobie-postgres" % DoobieV,
    "org.tpolecat" %% "doobie-h2" % DoobieV,
    "io.hydrosphere" %% "typed-sql" % "0.1.0"
  )

  val grpc: Seq[ModuleID] = Seq(
    "io.grpc" % "grpc-netty" % "1.18.0",
    "io.hydrosphere" %% "serving-grpc-scala" % "2.2.0"
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
  
  val fs2: Seq[ModuleID] = Seq(
    "co.fs2" %% "fs2-core" % "1.0.3",
    "co.fs2" %% "fs2-io" % "1.0.3",
    "io.github.dmateusp" %% "fs2-aws" % "0.27.3"
  )
  
  val math: Seq[ModuleID] = Seq(
    "org.scalanlp" %% "breeze" % "0.13.2"
  )

  val refined: Seq[ModuleID] = Seq(
    "eu.timepit" %% "refined" % "0.9.3"
  )

  val test: Seq[ModuleID] = Seq(
    "org.scalactic" %% "scalactic" % "3.0.8",
    "org.scalatest" %% "scalatest" % "3.0.8" % "test",
    "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaV % Test
  )

  val flyway: ModuleID = "org.flywaydb" % "flyway-core" % "5.2.1"
  
  val hashing: ModuleID = "net.openhft" % "zero-allocation-hashing" % "0.8"
  
  val enumeratum: ModuleID = "com.beachape" %% "enumeratum" % "1.5.13"
  
  val influx: ModuleID = "com.paulgoldbaum" %% "scala-influxdb-client" % "0.6.1"
  
  val mongo: ModuleID = "org.mongodb.scala" %% "mongo-scala-driver" % "2.5.0"
  
  val nlp: ModuleID = "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2"

  val kite: ModuleID = "org.kitesdk" % "kite-data-core" % "1.1.0"
  
  val hadoop: Seq[ModuleID] = Seq(
    "org.apache.hadoop" % "hadoop-common" % HadoopV,
    "org.apache.hadoop" % "hadoop-aws" % HadoopV
  )
  
  val parquet: Seq[ModuleID] = Seq(
    "org.apache.parquet" % "parquet-common" % ParquetV,
    "org.apache.parquet" % "parquet-encoding" % ParquetV,
    "org.apache.parquet" % "parquet-avro" % ParquetV
  )

  val projectDeps: Seq[ModuleID] = 
    test ++ logback ++ circe ++ finch ++ doobie ++ grpc ++ pureconfig ++ akka ++ cats ++ fs2 ++ math ++ refined ++ hadoop ++ parquet :+ 
      flyway :+ hashing :+ enumeratum :+ influx :+ mongo :+ nlp :+ kite
}
