package io.hydrosphere.sonar.utils

import cats.effect.{Async, IO}

import scala.concurrent.Future

object FutureOps {
  implicit class FutureLifts[A](f: Future[A]) {
    def liftToAsync[F[_]: Async]: F[A] = Async[F].liftIO(IO.fromFuture(IO(f)))
  }
}
