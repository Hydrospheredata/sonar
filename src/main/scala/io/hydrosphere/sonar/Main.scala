package io.hydrosphere.sonar

import java.util.concurrent.Executors

import akka.actor.typed.ActorSystem
import akka.util.Timeout
import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import com.twitter.finagle.Http
import doobie._
import io.grpc.netty.NettyServerBuilder
import io.grpc.{Channel, ClientInterceptors, ManagedChannelBuilder, Server}
import io.hydrosphere.serving.discovery.serving.ServingDiscoveryGrpc
import io.hydrosphere.serving.discovery.serving.ServingDiscoveryGrpc.ServingDiscovery
import io.hydrosphere.serving.gateway.api.{GatewayServiceGrpc, ServablePredictRequest}
import io.hydrosphere.serving.grpc.{AuthorityReplacerInterceptor, BuilderWrapper, Headers}
import io.hydrosphere.serving.manager.grpc.entities.ModelVersion
import io.hydrosphere.serving.monitoring.api.MonitoringServiceGrpc
import io.hydrosphere.serving.monitoring.api.MonitoringServiceGrpc.MonitoringService
import io.hydrosphere.serving.tensorflow.api.predict.PredictResponse
import io.hydrosphere.sonar.actors.MetricSpecDiscoverer.DiscoveryMsg
import io.hydrosphere.sonar.actors.{MetricSpecDiscoverer, SonarSupervisor}
import io.hydrosphere.sonar.config.{Configuration, H2, Postgres}
import io.hydrosphere.sonar.endpoints.{HttpService, MonitoringServiceGrpcApi}
import io.hydrosphere.sonar.services._
import io.hydrosphere.sonar.utils.GatewayServiceRpc
import org.flywaydb.core.Flyway
import org.h2.jdbcx.JdbcConnectionPool
import org.slf4j.bridge.SLF4JBridgeHandler
import pureconfig.generic.auto._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object Dependencies {
  
  def dbTransactor[F[_]: Async](config: Configuration)(implicit cs: ContextShift[F]): F[Transactor[F]] = config.db match {
    case H2(jdbcUrl, user, pass) => Async[F].delay(Transactor.fromDataSource[F](JdbcConnectionPool.create(jdbcUrl, user, pass), ExecutionContext.fromExecutor(Executors.newFixedThreadPool(32)), ExecutionContext.fromExecutor(Executors.newCachedThreadPool())))
    case Postgres(jdbcUrl, user, pass) => Async[F].delay(Transactor.fromDriverManager[F]("org.postgresql.Driver", jdbcUrl, user, pass))
  }
  
  def metricService[F[_]: Async](as: ActorSystem[DiscoveryMsg], timeout: Timeout): F[MetricSpecService[F]] = Async[F].delay {
      implicit val scheduler = as.scheduler
      implicit val t = timeout
      new MetricSpecServiceInterpreter[F](as)
    }

  def httpService[F[_]: Effect](metricSpecService: MetricSpecService[F], metricStorageService: MetricStorageService[F], profileStorageService: ProfileStorageService[F], modelDataService: ModelDataService[F], batchProfileService: BatchProfileService[F, fs2.Stream])(implicit cs: ContextShift[F]): F[HttpService[F]] = 
    Effect[F].delay(new HttpService[F](metricSpecService, metricStorageService, profileStorageService, modelDataService, batchProfileService))
  
  def modelDataService[F[_]: Async](config: Configuration): F[ModelDataService[F]] = for {
    state <- Ref.of[F, Map[Long, ModelVersion]](Map.empty)
    instance <- Async[F].delay(new ModelDataServiceGrpcInterpreter[F](config, state))
  } yield instance
  
  def predictionService[F[_]: Async](gtw: GatewayServiceRpc[F]): F[PredictionService[F]] =
    Async[F].delay(PredictionService.apply[F](gtw))
  
  def metricStorageService[F[_]: Async](config: Configuration): F[MetricStorageService[F]] = 
    Async[F].delay(new MetricStorageServiceInfluxInterpreter[F](config))
  
  def profileStorageService[F[_]: Async](config: Configuration): F[ProfileStorageService[F]] = for {
    state <- Ref.of[F, ProfileStorageServiceMongoInterpreter.ObjectIdState](Map.empty)
    instance <- Async[F].delay(new ProfileStorageServiceMongoInterpreter[F](config, state))
  } yield instance
  
  def batchProfileService(config: Configuration, profileStorageService: ProfileStorageService[IO]): IO[BatchProfileService[IO, fs2.Stream]] = {
    implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10)))
    for {
      state <- Ref.of[IO, Map[Long, BatchProfileService.ProcessingStatus]](Map.empty)
      instance <- IO.delay(new BatchProfileServiceInterpreter(config, state, profileStorageService))
    } yield instance
  }
}

object Main extends IOApp with Logging {

  def createActorSystem[F[_]: Sync](config: Configuration, metricSpecService: MetricSpecService[IO], modelDataService: ModelDataService[IO], predictionService: PredictionService[IO], metricStorageService: MetricStorageService[IO], profileStorageService: ProfileStorageService[IO]): F[ActorSystem[SonarSupervisor.Message]] =
    Sync[F].delay(ActorSystem[SonarSupervisor.Message](SonarSupervisor(config, metricSpecService, modelDataService, predictionService, metricStorageService, profileStorageService), "sonar"))

  def discoveryActorSystem[F[_]: Sync](reconnect: FiniteDuration,stub: ServingDiscovery): F[ActorSystem[DiscoveryMsg]] = Sync[F].delay {
    ActorSystem[DiscoveryMsg](MetricSpecDiscoverer(reconnect, stub), "serving-discovery")
  }

  def createGrpcChannel[F[_]: Sync](config: Configuration): F[Channel] = Sync[F].delay {
    val deadline = 2.minutes
    val builder = ManagedChannelBuilder.forAddress(config.sidecar.host, config.sidecar.grpcPort)
    builder.enableRetry()
    builder.usePlaintext()
    builder.keepAliveTimeout(deadline.length, deadline.unit)
    ClientInterceptors.intercept(builder.build(), new AuthorityReplacerInterceptor +: Headers.interceptors: _*)
  }

  def createDiscoveryStub[F[_]: Sync](channel: Channel): F[ServingDiscoveryGrpc.ServingDiscoveryStub] = Sync[F].delay {
    ServingDiscoveryGrpc.stub(channel)
  }

  def runGrpcServer[F[_]: Sync](config: Configuration, monitoringService: MonitoringService): F[Server] = Sync[F].delay {
    val builder = BuilderWrapper(NettyServerBuilder.forPort(config.grpc.port).maxInboundMessageSize(config.grpc.maxSize))
      .addService(MonitoringServiceGrpc.bindService(monitoringService, scala.concurrent.ExecutionContext.global))
    builder.build.start()
  }

  def loadConfiguration[F[_]: Sync]: F[Configuration] = Sync[F].delay(pureconfig.loadConfigOrThrow[Configuration])

  def runDbMigrations[F[_]: Sync](config: Configuration): F[Unit] = Sync[F].delay {
    Flyway
      .configure()
      .dataSource(config.db.jdbcUrl, config.db.user, config.db.pass)
      .load()
      .migrate()
  }

  def setupLogging[F[_] : Sync]: F[Unit] = Sync[F].delay {
    SLF4JBridgeHandler.removeHandlersForRootLogger()
    SLF4JBridgeHandler.install()
  }

  override def run(args: List[String]): IO[ExitCode] = for {
    _ <- setupLogging[IO]

    config <- loadConfiguration[IO]
    _ <- IO(logger.info(config.toString))

    grpcChannel <- createGrpcChannel[IO](config)
    gatewayRpc <- GatewayServiceRpc.make(grpcChannel)
    discoveryRpcStub <- createDiscoveryStub[IO](grpcChannel)

    discoveryAS <- discoveryActorSystem[IO](3.minute, discoveryRpcStub)

    metricSpecService <- Dependencies.metricService[IO](discoveryAS, 3.minutes)
    metricStorageService <- Dependencies.metricStorageService[IO](config)
    profileStorageService <- Dependencies.profileStorageService[IO](config)
    modelDataService <- Dependencies.modelDataService[IO](config)
    batchProfileService <- Dependencies.batchProfileService(config, profileStorageService)
    httpService <- Dependencies.httpService[IO](metricSpecService, metricStorageService, profileStorageService, modelDataService, batchProfileService)
    predictionService <- Dependencies.predictionService[IO](gatewayRpc)
    
    _ <- runDbMigrations[IO](config)
    
    actorSystem <- createActorSystem[IO](config, metricSpecService, modelDataService, predictionService, metricStorageService, profileStorageService)

    grpc <- runGrpcServer[IO](config, new MonitoringServiceGrpcApi(actorSystem))
    _ <- IO(logger.info(s"GRPC server started on port ${grpc.getPort}"))
    
    http <- IO(Http.server.withStreaming(true).serve(s"${config.http.host}:${config.http.port}", httpService.api))
    _ <- IO(logger.info(s"HTTP server started on ${http.boundAddress}"))
  } yield ExitCode.Success
}
