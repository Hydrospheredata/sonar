package io.hydrosphere.sonar.endpoints

import akka.actor.typed.ActorRef
import cats.effect.IO
import cats.implicits._
import com.google.protobuf.empty.Empty
import io.hydrosphere.serving.proto.manager.entities.MetricSpec
import io.hydrosphere.serving.proto.manager.entities.ThresholdConfig.CmpOp._
import io.hydrosphere.serving.proto.contract.tensor.Tensor
import io.hydrosphere.monitoring.proto.sonar.entities.ExecutionInformation
import io.hydrosphere.monitoring.proto.sonar.entities.ExecutionMetadata
import io.hydrosphere.monitoring.proto.sonar.api.MonitoringServiceGrpc.MonitoringService
import io.hydrosphere.sonar.Logging
import io.hydrosphere.sonar.actors.SonarSupervisor
import io.hydrosphere.sonar.services._
import io.hydrosphere.sonar.terms.{Check, ProfileSourceKind}
import io.hydrosphere.sonar.utils.checks.ProfileChecks

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class MonitoringServiceGrpcApi(
  recipient: ActorRef[SonarSupervisor.Message],
  profileStorageService: ProfileStorageService[IO],
  metricSpecService: MetricSpecService[IO],
  predictionService: PredictionService[IO],
  checkStorageService: CheckStorageService[IO],
  modelDataService: ModelDataService[IO],
  alertService: AlertService[IO]
) extends MonitoringService with Logging {

  override def analyze(executionInformation: ExecutionInformation): Future[Empty] = {
    Future {
      recipient ! SonarSupervisor.Request(executionInformation)
      // TODO: remove
      val maybeRequest = for {
        request <- executionInformation.request
        inputs = request.inputs
        response <- executionInformation.responseOrError.response
        outputs = response.outputs
      } yield inputs ++ outputs
      val effect = executionInformation.metadata match {
        case Some(md) =>
          for {
            modelVersion <- modelDataService.getModelVersion(md.modelVersionId)
            profiles <- profileStorageService.getProfiles(md.modelVersionId, ProfileSourceKind.Training)
            profileChecks = ProfileChecks.check(profiles, executionInformation)
            // TODO: get all metrics
            metricSpecs <- metricSpecService.getMetricSpecsByModelVersion(md.modelVersionId)
            // TODO: move to separated class
            metricChecks <- maybeRequest match {
              case Some(req) => processRequest(req, metricSpecs, md)
              case None => Map.empty[String, Check].pure[IO]
            }
            _ <- alertService.sendChecks(executionInformation, modelVersion, profileChecks, metricChecks).attempt // TODO(bulat): clarify if we need to ignore failures
            _ <- checkStorageService.saveCheckedRequest(executionInformation, modelVersion, profileChecks, metricChecks)
          } yield Unit
        case None => IO.unit // TODO: do something 
      }
      effect.unsafeRunAsync {
        case Left(value) => logger.error(s"error while saving checks", value)
        case Right(value) => //logger.info(s"${executionInformation.metadata.get.modelVersionId} checks are done")
      }
      Empty()
    }
  }

  private def processRequest(req: Map[String, Tensor], metricSpecs: List[MetricSpec], metadata: ExecutionMetadata) = {
    metricSpecs.traverse {
      case MetricSpec(id, name, _, config, _) =>
        val maybeTask = for {
          conf <- config
          servable <- conf.servable
        } yield {
          predictionService
            .predict(servable.name, req)
            .flatMap { predictResponse =>
              val value = predictResponse.outputs.get("value").flatMap(_.doubleVal.headOption).getOrElse(0d)
              config.get.threshold match {
                case Some(thresholdConfig) => IO.pure[(String, Check)](name -> (thresholdConfig.comparison match {
                  case EQ => Check(value == thresholdConfig.value, name, value, thresholdConfig.value, Some(id))
                  case NOT_EQ => Check(value != thresholdConfig.value, name, value, thresholdConfig.value, Some(id))
                  case GREATER => Check(value > thresholdConfig.value, name, value, thresholdConfig.value, Some(id))
                  case LESS => Check(value < thresholdConfig.value, name, value, thresholdConfig.value, Some(id))
                  case GREATER_EQ => Check(value >= thresholdConfig.value, name, value, thresholdConfig.value, Some(id))
                  case LESS_EQ => Check(value <= thresholdConfig.value, name, value, thresholdConfig.value, Some(id))
                  case Unrecognized(value) => throw new RuntimeException(s"Unrecognized cmp operator $value")
                }))
                case None => IO.raiseError[(String, Check)](new RuntimeException(s"${metadata.modelVersionId} cmpOperator is empty"))
              }
            }
            .attempt
        }
        maybeTask.getOrElse(IO.pure(Left(new Exception("Can't find servable for monitoring model"))))
    }
      // TODO: process errors
      .map(_.map {
        case Left(value) =>
          logger.error(s"Error while getting metrics", value)
          Left(value)
        case Right(value) =>
          Right(value)
      }.collect { case Right(value) => value }.toMap[String, Check])
  }

}