import sbt._

name := "sonar"

scalaVersion := "2.12.7"

version := sys.props.getOrElse("appVersion", "latest")

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

enablePlugins(sbtdocker.DockerPlugin)

dockerfile in docker := {
  val dockerFilesLocation = baseDirectory.value / "src/main/docker/"
  val jarFile: File = sbt.Keys.`package`.in(Compile, packageBin).value
  val classpath = (dependencyClasspath in Compile).value
  val artifactTargetPath = s"/app/app.jar"


  new Dockerfile {
    from("openjdk:alpine")

    label("SERVICE_ID", "-30")
    label("HS_SERVICE_MARKER", "HS_SERVICE_MARKER")
    label("DEPLOYMENT_TYPE", "APP")
    label("RUNTIME_ID", "-30")
    label("SERVICE_NAME", "monitoring")

    env("SIDECAR_PORT", "8081")
    env("SIDECAR_HOST", "sidecar")
    env("KAFKA_HOST", "kafka")
    env("KAFKA_PORT", "9092")
    env("APP_PORT", "9091")

    add(dockerFilesLocation, "/app/")
    cmd("/app/start.sh")
    run("chmod", "+x", "/app/start.sh")

    add(classpath.files, "/app/lib/")
    add(jarFile, artifactTargetPath)
  }
}

imageNames in docker := Seq(
  ImageName(s"docker.hydrosphere.io/${name.value}:latest"),

  ImageName(
    namespace = Some("docker.hydrosphere.io"),
    repository = name.value,
    tag = Some(version.value)
  )
)
