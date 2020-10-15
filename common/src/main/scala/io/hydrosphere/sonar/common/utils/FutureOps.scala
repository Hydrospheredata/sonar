package io.hydrosphere.sonar.common.utils

import cats.effect.{Async, ContextShift, IO}

import scala.concurrent.{ExecutionContext, Future}
import com.twitter.util.{Future => TwitterFuture}


object FutureOps {

  implicit final class FutureLifts[A](val f: Future[A]) extends AnyVal {
    def liftToAsync[F[_] : Async]: F[A] = {
      implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      Async[F].liftIO(IO.fromFuture(IO(f)))
    }
  }

  implicit final class TwitterFutureLifts[A](val f: TwitterFuture[A]) extends AnyVal {
    def liftToAsync[F[_] : Async]: F[A] = Async[F].async[A] { cb =>
      f.onSuccess(x => cb(Right(x)))
        .onFailure(x => cb(Left(x)))
    }
  }

}
