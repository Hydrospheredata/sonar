import sbt._
import sbtbuildinfo.{BuildInfoRenderer, ScalaCaseClassRenderer, ScalaCaseObjectRenderer}

name := "sonar"

scalaVersion := "2.12.7"

version := sys.props.getOrElse("appVersion", IO.read(file("version")).trim)

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
resolvers += Resolver.sonatypeRepo("releases")
resolvers += Resolver.sonatypeRepo("public")

libraryDependencies ++= Dependencies.projectDeps

cancelable in Global := true

enablePlugins(BuildInfoPlugin, sbtdocker.DockerPlugin)
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

docker / dockerfile := {
  val dockerFilesLocation = baseDirectory.value / "src/main/docker/"
  val jarFile: File = (Compile / packageBin / sbt.Keys.`package`).value
  val classpath = (Compile / dependencyClasspath).value
  val artifactTargetPath = "app.jar"


  new Dockerfile {
    from("openjdk:8u212-jre-alpine3.9")

    label("SERVICE_ID", "-30")
    label("HS_SERVICE_MARKER", "HS_SERVICE_MARKER")
    label("DEPLOYMENT_TYPE", "APP")
    label("RUNTIME_ID", "-30")
    label("SERVICE_NAME", "monitoring")
    label("maintainer", "support@hydrosphere.io")

    env("KAFKA_HOST", "kafka")
    env("KAFKA_PORT", "9092")
    env("APP_PORT", "9091")
    
    run("apk", "update")
    run("apk", "add", "--no-cache", "freetype>=2.9.1-r3", "krb5-libs>=1.15.5-r1", "libbz2>=1.0.6-r7", 
        "libcom_err>=1.44.5-r2", "libcrypto1.1>=1.1.1k-r0", "libjpeg-turbo>=1.5.3-r6", "libssl1.1>=1.1.1k-r0", 
        "libtasn1>=4.14", "libx11>=1.6.12-r0", "musl>=1.1.20-r6", "openjdk8-jre>=8.272.10-r0", 
        "openjdk8-jre-base>=8.272.10-r0", "openjdk8-jre-lib>=8.272.10-r0", "sqlite-libs>=3.28.0-r3")
    // run("apk", "update")
    // run("apk", "add", "--no-cache", "libc6-compat", "nss")

    workDir("/app/")
    
    copy(dockerFilesLocation, "./", "daemon:daemon")
    copy(classpath.files, "./lib/", "daemon:daemon")
    copy(jarFile, artifactTargetPath, "daemon:daemon")
    run("chmod", "+x", "start.sh")

    user("daemon")

    cmd("/app/start.sh")
  }
}

docker / imageNames := Seq(
  ImageName(s"hydrosphere/${name.value}:${version.value}")
)

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion, git.gitCurrentBranch, git.gitCurrentTags, git.gitHeadCommit)
buildInfoPackage := "io.hydrosphere.sonar"
buildInfoOptions += BuildInfoOption.ToJson
buildInfoRenderFactory := ScalaCaseClassRenderer.apply
Compile / unmanagedSourceDirectories += sourceManaged.value

excludeDependencies += "org.slf4j" % "slf4j-log4j12"
