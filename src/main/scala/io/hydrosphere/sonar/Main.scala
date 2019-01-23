package io.hydrosphere.sonar

import java.util.concurrent.Executors

import akka.actor.typed.ActorSystem
import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import com.twitter.finagle.Http
import doobie._
import io.grpc.Server
import io.grpc.netty.NettyServerBuilder
import io.hydrosphere.serving.grpc.BuilderWrapper
import io.hydrosphere.serving.manager.grpc.entities.ModelVersion
import io.hydrosphere.serving.monitoring.monitoring.MonitoringServiceGrpc
import io.hydrosphere.serving.monitoring.monitoring.MonitoringServiceGrpc.MonitoringService
import io.hydrosphere.sonar.actors.SonarSupervisor
import io.hydrosphere.sonar.config.{Configuration, H2, Postgres}
import io.hydrosphere.sonar.endpoints.{HttpService, MonitoringServiceGrpcApi}
import io.hydrosphere.sonar.services._
import org.flywaydb.core.Flyway
import org.h2.jdbcx.JdbcConnectionPool
import org.slf4j.bridge.SLF4JBridgeHandler
import pureconfig.generic.auto._

import scala.concurrent.ExecutionContext

object Dependencies {
  
  def dbTransactor[F[_]: Async](config: Configuration)(implicit cs: ContextShift[F]): F[Transactor[F]] = config.db match {
    case H2(jdbcUrl, user, pass) => Async[F].delay(Transactor.fromDataSource[F](JdbcConnectionPool.create(jdbcUrl, user, pass), ExecutionContext.fromExecutor(Executors.newFixedThreadPool(32)), ExecutionContext.fromExecutor(Executors.newCachedThreadPool())))
    case Postgres(jdbcUrl, user, pass) => Async[F].delay(Transactor.fromDriverManager[F]("org.postgresql.Driver", jdbcUrl, user, pass))
  }
  
  def metricService[F[_]: Sync](transactor: Transactor[F]): F[MetricSpecService[F]] = 
    Sync[F].delay(new MetricSpecServiceInterpreter[F](transactor))
 
  def httpService[F[_]: Effect](metricSpecService: MetricSpecService[F]): F[HttpService[F]] = 
    Effect[F].delay(new HttpService[F](metricSpecService))
  
  def modelDataService[F[_]: Async](config: Configuration): F[ModelDataService[F]] = for {
    state <- Ref.of[F, Map[Long, ModelVersion]](Map.empty)
    instance <- Async[F].delay(new ModelDataServiceGrpcInterpreter[F](config, state))
  } yield instance
  
  def predictionService[F[_]: Async](config: Configuration): F[PredictionService[F]] = 
    Async[F].delay(new PredictionServiceGrpcInterpreter[F](config))
  
  def metricStorageService[F[_]: Sync](config: Configuration): F[MetricStorageService[F]] = 
    Sync[F].delay(new MetricStorageServiceInfluxInterpreter[F](config))
  
  def profileStorageService[F[_]: Async](config: Configuration): F[ProfileStorageService[F]] = for {
    state <- Ref.of[F, ProfileStorageServiceMongoInterpreter.ObjectIdState](Map.empty)
    instance <- Async[F].delay(new ProfileStorageServiceMongoInterpreter[F](config, state))
  } yield instance
}

object Main extends IOApp with Logging {

  def createActorSystem[F[_]: Sync](config: Configuration, metricSpecService: MetricSpecService[IO], modelDataService: ModelDataService[IO], predictionService: PredictionService[IO], metricStorageService: MetricStorageService[IO], profileStorageService: ProfileStorageService[IO]): F[ActorSystem[SonarSupervisor.Message]] =
    Sync[F].delay(ActorSystem[SonarSupervisor.Message](SonarSupervisor(config, metricSpecService, modelDataService, predictionService, metricStorageService, profileStorageService), "sonar"))

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
    
    transactor <- Dependencies.dbTransactor[IO](config)
    metricSpecService <- Dependencies.metricService[IO](transactor)
    httpService <- Dependencies.httpService[IO](metricSpecService)
    modelDataService <- Dependencies.modelDataService[IO](config)
    predictionService <- Dependencies.predictionService[IO](config)
    metricStorageService <- Dependencies.metricStorageService[IO](config)
    profileStorageService <- Dependencies.profileStorageService[IO](config)
    
    _ <- runDbMigrations[IO](config)
    
    actorSystem <- createActorSystem[IO](config, metricSpecService, modelDataService, predictionService, metricStorageService, profileStorageService)
    
    grpc <- runGrpcServer[IO](config, new MonitoringServiceGrpcApi(actorSystem))
    _ <- IO(logger.info(s"GRPC server started on port ${grpc.getPort}"))
    
    http <- IO(Http.serve(s"${config.http.host}:${config.http.port}", httpService.api))
    _ <- IO(logger.info(s"HTTP server started on ${http.boundAddress}"))
  } yield ExitCode.Success
}
