import sbt.Keys.scalacOptions
import sbt.{addCompilerPlugin, _}
import sbtbuildinfo.{BuildInfoRenderer, ScalaCaseClassRenderer, ScalaCaseObjectRenderer}

name := "sonar"

scalaVersion := "2.13.3"

version := sys.props.getOrElse("appVersion", "latest")

//scalacOptions ++= Seq(
lazy val compilerOptions = Seq(
  "-encoding", "UTF-8",
  "-unchecked",
  "-deprecation",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-language:experimental.macros"
)

libraryDependencies ++= Dependencies.projectDeps

cancelable in Global := true

//enablePlugins(BuildInfoPlugin, sbtdocker.DockerPlugin)

lazy val dockerSettings = Seq(
  dockerfile in docker := {
    val dockerFilesLocation = baseDirectory.value / "legacy/src/main/docker/" // TODO: move dockerfile into the root directory
    val jarFile: File = sbt.Keys.`package`.in(Compile, packageBin).value
    val classpath = (dependencyClasspath in Compile).value
    val artifactTargetPath = s"/app/app.jar"
  
  
    new Dockerfile {
      from("openjdk:11.0.8-jre-slim")
  
      label("SERVICE_ID", "-30")
      label("HS_SERVICE_MARKER", "HS_SERVICE_MARKER")
      label("DEPLOYMENT_TYPE", "APP")
      label("RUNTIME_ID", "-30")
      label("SERVICE_NAME", "monitoring")
  
      env("KAFKA_HOST", "kafka")
      env("KAFKA_PORT", "9092")
      env("APP_PORT", "9091")
      
      run("apk", "update")
      run("apk", "add", "--no-cache", "libc6-compat", "nss")
  
      add(dockerFilesLocation, "/app/")
      entryPoint("/app/start.sh")
      run("chmod", "+x", "/app/start.sh")
  
  
      add(classpath.files, "/app/lib/")
      add(jarFile, artifactTargetPath)
    }
  },
  
  imageNames in docker := Seq(
    ImageName(
      namespace = Some("harbor.hydrosphere.io/hydro-serving"),
      repository = name.value,
      tag = Some(version.value)
    )
  )
)

lazy val globalResources = file("resources")

lazy val buildInfoSettings = Seq(
  buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion, git.gitCurrentBranch, git.gitCurrentTags, git.gitHeadCommit),
  buildInfoPackage := "io.hydrosphere.sonar",
  buildInfoOptions += BuildInfoOption.ToJson,
  buildInfoRenderFactory := ScalaCaseClassRenderer.apply,
  unmanagedSourceDirectories in Compile += sourceManaged.value
)

lazy val mappingSettings = Seq(mappings in(Compile, packageBin) ++= {
  val commonM = (mappings in(common, Compile, packageBin)).value
  val legacyM = (mappings in(legacy, Compile, packageBin)).value
  val ingestionM = (mappings in(ingestion, Compile, packageBin)).value
  commonM ++ legacyM ++ ingestionM
})

lazy val commonSettings = Seq(
  scalaVersion := "2.13.3",
  libraryDependencies ++= Dependencies.projectDeps,
  scalacOptions ++= compilerOptions,
  excludeDependencies += "org.slf4j" % "slf4j-log4j12"
)

lazy val scalafixSettings = Seq(
  scalafixDependencies in ThisBuild += "org.scala-lang.modules" %% "scala-collection-migrations" % "2.2.0", 
  addCompilerPlugin(scalafixSemanticdb), 
  scalacOptions ++= List("-Yrangepos", "-P:semanticdb:synthetics:on")
)

lazy val root = project
  .in(file("."))
  .enablePlugins(sbtdocker.DockerPlugin)
  .settings(mappingSettings ++ dockerSettings ++ commonSettings, unmanagedResourceDirectories in Runtime += globalResources, unmanagedResourceDirectories in Compile += globalResources)
  .aggregate(common, legacy, ingestion)

lazy val common = project
  .in(file("common"))
  .settings(name := "common", commonSettings, scalafixSettings)

lazy val legacy = project
  .in(file("legacy"))
  .enablePlugins(BuildInfoPlugin)
  .settings(name := "legacy", commonSettings, buildInfoSettings, scalafixSettings)
  .dependsOn(common)

lazy val ingestion = project
  .in(file("ingestion"))
  .settings(name := "ingestion", commonSettings, scalafixSettings)
  .dependsOn(common, legacy)
