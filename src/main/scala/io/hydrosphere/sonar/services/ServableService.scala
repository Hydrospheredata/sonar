package io.hydrosphere.sonar.services

import cats.data.{State, StateT}
import cats.effect.{Concurrent, ConcurrentEffect, Effect, Sync}
import cats.effect.implicits._
import fs2.concurrent.Queue
import io.grpc.Channel
import io.grpc.stub.StreamObserver
import io.hydrosphere.serving.discovery.serving.{ServableDiscoveryEvent, ServingDiscoveryGrpc}
import io.hydrosphere.serving.discovery.serving.ServingDiscoveryGrpc.ServingDiscovery
import io.hydrosphere.serving.manager.api.ManagerServiceGrpc
import io.hydrosphere.serving.manager.api.ManagerServiceGrpc.ManagerService
import io.hydrosphere.serving.manager.grpc.entities.Servable

trait ServableService[F[_]] {
  def create(): F[String]

  def kill(name: String): F[Unit]
}

object ServableService {
  def apply[F[_]](grpcChannel: Channel)(implicit F: Sync[F]): ServableService[F] = {
    val client = ManagerServiceGrpc.stub(grpcChannel)
    forClient(client)
  }

  def forClient[F[_]](client: ManagerService)(implicit F: Sync[F]): ServableService[F] = ???
}