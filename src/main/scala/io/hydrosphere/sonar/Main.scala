package io.hydrosphere.sonar

import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.Scheduler
import akka.actor.typed.ActorSystem
import akka.util.Timeout
import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import com.mongodb.{ConnectionString, MongoCredential}
import com.mongodb.MongoCredential.createCredential
import com.twitter.finagle.Http
import io.grpc.netty.NettyServerBuilder
import io.grpc.{Channel, ClientInterceptors, ManagedChannelBuilder, Server}
import io.hydrosphere.serving.discovery.serving.ServingDiscoveryGrpc
import io.hydrosphere.serving.discovery.serving.ServingDiscoveryGrpc.ServingDiscovery
import io.hydrosphere.serving.grpc.{AuthorityReplacerInterceptor, BuilderWrapper, Headers}
import io.hydrosphere.serving.manager.grpc.entities.ModelVersion
import io.hydrosphere.serving.monitoring.api.MonitoringServiceGrpc
import io.hydrosphere.serving.monitoring.api.MonitoringServiceGrpc.MonitoringService
import io.hydrosphere.sonar.actors.MetricSpecDiscoverer.DiscoveryMsg
import io.hydrosphere.sonar.actors.{MetricSpecDiscoverer, SonarSupervisor}
import io.hydrosphere.sonar.config.Configuration
import io.hydrosphere.sonar.endpoints.{HttpService, MonitoringServiceGrpcApi}
import io.hydrosphere.sonar.services._
import io.hydrosphere.sonar.utils.{AutoODServiceRpc, GatewayServiceRpc}
import org.mongodb.scala.{MongoClient, MongoClientSettings}
import org.slf4j.bridge.SLF4JBridgeHandler
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderException
import pureconfig.generic.auto._
import eu.timepit.refined.pureconfig._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object Dependencies {

  def mongoClient[F[_]: Sync](config: Configuration): F[MongoClient] = {
    Sync[F].delay {
      val builder = MongoClientSettings
        .builder()
        .applyToClusterSettings(b => b.applyConnectionString(new ConnectionString(s"mongodb://${config.mongo.host}:${config.mongo.port}/${config.mongo.database}?authSource=admin")))
        .applyToConnectionPoolSettings(b => b.maxSize(200).maxWaitTime(100, TimeUnit.SECONDS))
      val credentials: Option[MongoCredential] = for {
        user <- config.mongo.user
        pass <- config.mongo.pass
        authDb <- config.mongo.authDb
      } yield createCredential(user, authDb, pass.toCharArray)

      credentials match {
        case Some(creds) => builder.credential(creds)
        case None =>
      }

      val settings = builder.build()
      MongoClient(settings)
    }
  }
  
  def metricService[F[_]: Async](as: ActorSystem[DiscoveryMsg], timeout: Timeout): F[MetricSpecService[F]] = Async[F].delay {
      implicit val scheduler: Scheduler = as.scheduler
      implicit val t: Timeout = timeout
      new MetricSpecServiceInterpreter[F](as)
    }

  def httpService[F[_]: Effect](metricSpecService: MetricSpecService[F], profileStorageService: ProfileStorageService[F], modelDataService: ModelDataService[F], batchProfileService: TrainingProfileService[F, fs2.Stream], checkStorageService: CheckStorageService[F], checkSlowStorageService: CheckSlowStorageService[F])(implicit cs: ContextShift[F]): F[HttpService[F]] =
    Effect[F].delay(new HttpService[F](metricSpecService, profileStorageService, modelDataService, batchProfileService, checkStorageService, checkSlowStorageService))
  
  def modelDataService[F[_]: Async](config: Configuration): F[ModelDataService[F]] = for {
    state <- Ref.of[F, Map[Long, ModelVersion]](Map.empty)
    instance <- Async[F].delay(new ModelDataServiceGrpcInterpreter[F](config, state))
  } yield instance
  
  def predictionService[F[_]: Async](gtw: GatewayServiceRpc[F]): F[PredictionService[F]] =
    Async[F].delay(PredictionService.apply[F](gtw))
  
  def profileStorageService[F[_]: Async](config: Configuration, client: MongoClient): F[ProfileStorageService[F]] = for {
    state <- Ref.of[F, ProfileStorageServiceMongoInterpreter.ObjectIdState](Map.empty)
    instance <- Async[F].delay(new ProfileStorageServiceMongoInterpreter[F](config, state, client))
  } yield instance
  
  def batchProfileService(config: Configuration, profileStorageService: ProfileStorageService[IO], mongoClient: MongoClient, autoODService: AutoODService[IO]): IO[TrainingProfileService[IO, fs2.Stream]] = {
    implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10)))
    for {
      state <- Ref.of[IO, Map[Long, TrainingProfileService.ProcessingStatus]](Map.empty)
      instance <- IO.delay(new TrainingProfileServiceInterpreter(config, state, profileStorageService, mongoClient, autoODService))
    } yield instance
  }

  def checkStorageService[F[_]: Async](configuration: Configuration, client: MongoClient, modelDataService: ModelDataService[F]): F[CheckStorageService[F]] = for {
    instance <- Async[F].delay(new MongoCheckStorageService[F](configuration, client, modelDataService))
  } yield instance

  def alertManagerService(config: Configuration): AlertService[IO] = {
    config.alerting match {
      case Some(value) => new PrometheusAMService[IO](value.alertManagerUrl, value.frontendUrl)
      case None => new NoopAlertService[IO]
    }
  }

  def batchMetricService[F[_]: Async](configuration: Configuration, mongoClient: MongoClient, modelDataService: ModelDataService[IO], checkStorageService: CheckStorageService[IO]): F[BatchMetricService[F]] =
    Async[F].delay{
      new MongoParquetBatchMetricService[F](configuration, mongoClient, modelDataService, checkStorageService)
    }
  
  def getCheckSlowStorageService[F[_]: Async](configuration: Configuration, modelDataService: ModelDataService[F], checkStorageService: CheckStorageService[F]): F[CheckSlowStorageService[F]] = {
    val slowStorageService = new S3ParquetSlowStorageService[F](configuration, modelDataService, checkStorageService)
    slowStorageService.createBucketIfNotExists().as(slowStorageService)
  }

  def autoODService[F[_]: Async](client: AutoODServiceRpc[F]): F[AutoODService[F]] = Async[F].delay(new GRPCAutoODService[F](client))
}

object Main extends IOApp with Logging {

  def createActorSystem[F[_]: Sync](config: Configuration, metricSpecService: MetricSpecService[IO], modelDataService: ModelDataService[IO], predictionService: PredictionService[IO], profileStorageService: ProfileStorageService[IO]/*, alertManagerService: AlertManagerService[IO]*/): F[ActorSystem[SonarSupervisor.Message]] =
    Sync[F].delay(ActorSystem[SonarSupervisor.Message](SonarSupervisor(config, metricSpecService, modelDataService, predictionService, profileStorageService/*, alertManagerService*/), "sonar"))

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

  def loadConfiguration[F[_]: Sync]: F[Configuration] = Sync[F].fromEither(ConfigSource.default.load[Configuration].leftMap(ConfigReaderException(_)))

  def setupLogging[F[_] : Sync]: F[Unit] = Sync[F].delay {
    SLF4JBridgeHandler.removeHandlersForRootLogger()
    SLF4JBridgeHandler.install()
  }

  override def run(args: List[String]): IO[ExitCode] = for {
    _ <- setupLogging[IO]
    
    config <- loadConfiguration[IO]
    _ <- IO(logger.info(config.toString))
    mongoClient <- Dependencies.mongoClient[IO](config)
    grpcChannel <- createGrpcChannel[IO](config)
    gatewayRpc = GatewayServiceRpc.make[IO](grpcChannel)
    discoveryRpcStub <- createDiscoveryStub[IO](grpcChannel)

    discoveryAS <- discoveryActorSystem[IO](3.minute, discoveryRpcStub)
    
    autoODRpc = AutoODServiceRpc.make[IO](grpcChannel)

    autoOdService <- Dependencies.autoODService[IO](autoODRpc) 
    metricSpecService <- Dependencies.metricService[IO](discoveryAS, 3.minutes)
    profileStorageService <- Dependencies.profileStorageService[IO](config, mongoClient)
    modelDataService <- Dependencies.modelDataService[IO](config)
    batchProfileService <- Dependencies.batchProfileService(config, profileStorageService, mongoClient, autoOdService)
    checkStorageService <- Dependencies.checkStorageService[IO](config, mongoClient, modelDataService)
    checkSlowService <- Dependencies.getCheckSlowStorageService[IO](config, modelDataService, checkStorageService)
    httpService <- Dependencies.httpService[IO](metricSpecService, profileStorageService, modelDataService, batchProfileService, checkStorageService, checkSlowService)
    predictionService <- Dependencies.predictionService[IO](gatewayRpc)
    amService = Dependencies.alertManagerService(config)
    writerService <- Dependencies.batchMetricService[IO](config, mongoClient, modelDataService, checkStorageService)
    
    _ <- writerService.start

    actorSystem <- createActorSystem[IO](config, metricSpecService, modelDataService, predictionService, profileStorageService)

    grpc <- runGrpcServer[IO](config, new MonitoringServiceGrpcApi(actorSystem, profileStorageService, metricSpecService, predictionService, checkStorageService, modelDataService, amService))
    _ <- IO(logger.info(s"GRPC server started on port ${grpc.getPort}"))

    http <- IO(Http.server.withStreaming(true).serve(s"${config.http.host}:${config.http.port}", httpService.api))
    _ <- IO(logger.info(s"HTTP server started on ${http.boundAddress}"))
  } yield ExitCode.Success
}
