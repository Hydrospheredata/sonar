package io.hydrosphere.sonar.utils.mongo

import cats.effect.Async
import org.mongodb.scala.{FindObservable, SingleObservable}
import io.hydrosphere.sonar.utils.FutureOps._

object ObservableExt {

  implicit final class SingleIOObservable[T](observable: SingleObservable[T]) {
    def toF[F[_]](implicit F: Async[F]): F[T] = {
      observable.toFuture().liftToAsync[F]
    }
  }

  implicit final class FindIOObservable[T](observable: FindObservable[T]) {
    def toF[F[_]](implicit F: Async[F]): F[Seq[T]] = {
      observable.toFuture().liftToAsync
    }
  }

}
