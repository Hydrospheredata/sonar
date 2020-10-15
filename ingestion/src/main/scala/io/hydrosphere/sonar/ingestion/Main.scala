package io.hydrosphere.sonar.ingestion

import cats.effect.{ExitCode, IO, IOApp}
import io.hydrosphere.sonar.common.utils.Logging

object Main extends IOApp with Logging {
  override def run(args: List[String]): IO[ExitCode] = for {
    _ <- IO(println(ClassLoader.getSystemResource("application.conf")))
    _ <- IO(println(ClassLoader.getSystemResource("logback.xml")))
    _ <- IO(logger.info("And here we go"))
  } yield ExitCode.Success
}