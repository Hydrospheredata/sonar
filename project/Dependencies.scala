import sbt._

object Dependencies {


  val LogbackV = "1.2.3"
  val logback: Seq[ModuleID] = Seq(
    "ch.qos.logback" % "logback-classic" % LogbackV,
    "ch.qos.logback" % "logback-core" % LogbackV,
    "org.slf4j" % "jul-to-slf4j" % "1.7.25"
  )

  val CirceV = "0.11.1"
  val circe: Seq[ModuleID] = Seq(
    "io.circe" %% "circe-generic" % CirceV,
    "io.circe" %% "circe-parser" % CirceV,
    "io.circe" %% "circe-generic-extras" % CirceV
  )

  val FinchV = "0.31.0"
  val finch: Seq[ModuleID] = Seq(
    "com.github.finagle" %% "finchx-core" % FinchV,
    "com.github.finagle" %% "finchx-circe" % FinchV,
    "com.github.finagle" %% "finchx-fs2" % FinchV,
    "com.github.finagle" %% "finchx-iteratee" % FinchV
  )

  val grpc: Seq[ModuleID] = Seq(
    "io.grpc" % "grpc-netty" % "1.18.0",
    "io.hydrosphere" %% "serving-grpc-scala" % "2.4.0-rc2"
  )

  val PureConfigV = "0.12.1"
  val pureconfig: Seq[ModuleID] = Seq(
    "com.github.pureconfig" %% "pureconfig" % PureConfigV
  )

  val AkkaV = "2.5.27"
  val akka: Seq[ModuleID] = Seq(
    "com.typesafe.akka" %% "akka-actor-typed" % AkkaV,
    "com.typesafe.akka" %% "akka-slf4j" % AkkaV,
    "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaV % Test
  )

  val CatsV = "2.0.0"
  val cats: Seq[ModuleID] = Seq(
    "org.typelevel" %% "cats-core" % CatsV,
    "org.typelevel" %% "cats-effect" % CatsV
  )
  
  // TODO: update to 2.0
  val fs2: Seq[ModuleID] = Seq(
    "co.fs2" %% "fs2-core" % "1.0.5",
    "co.fs2" %% "fs2-io" % "1.0.5",
    "io.github.dmateusp" %% "fs2-aws" % "0.27.3" exclude("com.amazonaws", "aws-java-sdk-bundle")
  )

  val TestV = "3.0.8"
  val test: Seq[ModuleID] = Seq(
    "org.scalactic" %% "scalactic" % TestV,
    "org.scalatest" %% "scalatest" % TestV % "test"
  )

  val HadoopV = "3.2.1"
  val hadoop: Seq[ModuleID] = Seq(
    "org.apache.hadoop" % "hadoop-common" % HadoopV,
    "org.apache.hadoop" % "hadoop-aws" % HadoopV exclude("com.amazonaws", "aws-java-sdk-bundle"),
    "org.apache.hadoop" % "hadoop-mapreduce-client-core" % HadoopV
  )

  val ParquetV = "1.11.0"
  val parquet: Seq[ModuleID] = Seq(
    "org.apache.parquet" % "parquet-common" % ParquetV,
    "org.apache.parquet" % "parquet-encoding" % ParquetV,
    "org.apache.parquet" % "parquet-avro" % ParquetV
  )

  val RefinedV = "0.9.14"
  val refined: Seq[ModuleID]  = Seq (
    "eu.timepit" %% "refined"                 % RefinedV,
    "eu.timepit" %% "refined-cats"            % RefinedV,
    "eu.timepit" %% "refined-pureconfig"      % RefinedV,
    "io.circe"   %% "circe-refined"           % CirceV
  )

  val projectDeps: Seq[ModuleID] = 
    test ++ logback ++ circe ++ finch ++ grpc ++ pureconfig ++ akka ++ cats ++ fs2 ++ hadoop ++ parquet ++ refined ++
    Seq(
      "net.openhft" % "zero-allocation-hashing" % "0.8",
      "org.scalanlp" %% "breeze" % "0.13.2",
      "com.beachape" %% "enumeratum" % "1.5.13",
      "org.mongodb.scala" %% "mongo-scala-driver" % "4.1.0",
      "com.amazonaws" % "aws-java-sdk-bundle" % "1.11.456",
      "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2",
      "tech.allegro.schema.json2avro" % "converter" % "0.2.9",
      "io.minio" % "minio" % "6.0.11"
    )
}
