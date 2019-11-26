package io.hydrosphere.sonar.endpoints

import akka.actor.typed.ActorRef
import cats.effect.IO
import cats.implicits._
import com.google.protobuf.empty.Empty
import io.hydrosphere.serving.monitoring.api.ExecutionInformation
import io.hydrosphere.serving.monitoring.api.MonitoringServiceGrpc.MonitoringService
import io.hydrosphere.sonar.Logging
import io.hydrosphere.sonar.actors.SonarSupervisor
import io.hydrosphere.sonar.services.{CheckStorageService, MetricSpecService, ModelDataService, PredictionService, ProfileStorageService}
import io.hydrosphere.sonar.terms.{Check, CustomModelMetricSpec, Eq, Greater, GreaterEq, Less, LessEq, NotEq, ProfileSourceKind}
import io.hydrosphere.sonar.utils.ExecutionInformationOps._
import io.hydrosphere.sonar.utils.checks.ProfileChecks

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

// TODO: replace IO with F[_]
class MonitoringServiceGrpcApi(recipient: ActorRef[SonarSupervisor.Message], profileStorageService: ProfileStorageService[IO], metricSpecService: MetricSpecService[IO], predictionService: PredictionService[IO], checkStorageService: CheckStorageService[IO], modelDataService: ModelDataService[IO]) extends MonitoringService with Logging {
  override def analyze(executionInformation: ExecutionInformation): Future[Empty] = {
    Future {
      logger.info("Got executionInformation from GRPC")
      recipient ! SonarSupervisor.Request(executionInformation)
      // TODO: remove
      val maybeRequest = for {
        request <- executionInformation.request
        inputs = request.inputs
        response <- executionInformation.responseOrError.response
        outputs = response.outputs
      } yield inputs ++ outputs
      logger.info(s"got ${maybeRequest.getOrElse(Map.empty).keys.mkString(",")}")
      val effect = executionInformation.metadata match {
        case Some(md) =>
          for {
            modelVersion <- modelDataService.getModelVersion(md.modelVersionId)
            _ = logger.info(s"${md.modelVersionId} got modelVersion")
            profiles <- profileStorageService.getProfiles(md.modelVersionId, ProfileSourceKind.Training)
            _ = logger.info(s"${md.modelVersionId} got profiles")
            profileChecks = ProfileChecks.check(profiles, executionInformation)
            _ = logger.info(s"${md.modelVersionId} computed checks")
            // TODO: get all metrics
            metricSpecs <- metricSpecService.getMetricSpecsByModelVersion(md.modelVersionId)
            _ = logger.info(s"${md.modelVersionId} got metricspecs: ${metricSpecs}")
            // TODO: move to separated class
            metricChecks <- maybeRequest match {
              case Some(req) => metricSpecs.collect {
                case CustomModelMetricSpec(name, modelVersionId, config, withHealth, id) =>
                  predictionService
                    .predict(config.servableName, req)
                    .flatMap { predictResponse =>
                      val value = predictResponse.outputs.get("value").flatMap(_.doubleVal.headOption).getOrElse(0d)
                      config.thresholdCmpOperator match {
                        case Some(cmpOperator) => IO.pure[Check](cmpOperator match {
                          case Eq => Check(value == config.threshold.getOrElse(Double.MaxValue), name, value, config.threshold.getOrElse(Double.MaxValue), Some(id))
                          case NotEq => Check(value != config.threshold.getOrElse(Double.MaxValue), name, value, config.threshold.getOrElse(Double.MaxValue), Some(id))
                          case Greater => Check(value > config.threshold.getOrElse(Double.MaxValue), name, value, config.threshold.getOrElse(Double.MaxValue), Some(id))
                          case Less => Check(value < config.threshold.getOrElse(Double.MinValue), name, value, config.threshold.getOrElse(Double.MinValue), Some(id))
                          case GreaterEq => Check(value >= config.threshold.getOrElse(Double.MaxValue), name, value, config.threshold.getOrElse(Double.MaxValue), Some(id))
                          case LessEq => Check(value <= config.threshold.getOrElse(Double.MinValue), name, value, config.threshold.getOrElse(Double.MinValue), Some(id))
                        })
                        case None => IO.raiseError[Check](new RuntimeException(s"${md.modelVersionId} cmpOperator is empty"))
                      }
                    }
                    .attempt
              }
                // TODO: process errors
                .sequence.map(_.filter(_.isRight).map(_.right.get))
              case None => IO.pure(Seq.empty)
            }
            _ <- checkStorageService.saveCheckedRequest(executionInformation, modelVersion, profileChecks + ("overall" -> metricChecks))
          } yield Unit
        case None => IO.unit // TODO: do something 
      }
      effect.unsafeRunAsync {
        case Left(value) => logger.error(s"error while saving checks", value)
        case Right(value) => logger.info(s"${executionInformation.metadata.get.modelVersionId} checks are done")
      }
      Empty()
    }
  }
}
