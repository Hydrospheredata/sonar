package io.hydrosphere.sonar.endpoints

import cats._
import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._
import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response}
import io.circe._
import io.circe.generic.extras.{Configuration => CirceExtraConfiguration}
import io.circe.generic.extras.auto._
import io.finch._
import io.finch.circe._
import io.hydrosphere.sonar.services.{MetricSpecService, MetricStorageService, ProfileStorageService}
import io.hydrosphere.sonar.utils.MetricSpecNotFound
import io.hydrosphere.sonar.Logging
import io.hydrosphere.sonar.terms.{MetricSpec, Profile, ProfileSourceKind}

import scala.util.control.NonFatal

case class ProfileResponse(training: Option[Profile], production: Option[Profile])

//noinspection TypeAnnotation
class HttpService[F[_] : Monad : Effect](metricSpecService: MetricSpecService[F], metricStorageService: MetricStorageService[F], profileStorageService: ProfileStorageService[F]) extends Logging with Endpoint.Module[F] {

  implicit val genDevConfig: CirceExtraConfiguration =
    CirceExtraConfiguration.default.withDefaults.withDiscriminator("kind")

  def encodeErrorList(es: NonEmptyList[Exception]): Json = {
    val messages = es.map(x => Json.fromString(x.getMessage)).toList
    Json.obj("errors" -> Json.arr(messages: _*))
  }

  implicit val encodeException: Encoder[Exception] = Encoder.instance({
    case e: io.finch.Errors => encodeErrorList(e.errors)
    case e: io.finch.Error =>
      e.getCause match {
        case e: io.circe.Errors => encodeErrorList(e.errors)
        case _ => encodeErrorList(NonEmptyList.one(e))
      }
    case e: Exception => encodeErrorList(NonEmptyList.one(e))
  })

  def healthCheck: Endpoint[F, String] = get("health") {
    Ok("ok").pure[F]
  }

  def createMetricSpec = post("metricspec" :: jsonBody[MetricSpec]) { metricSpec: MetricSpec =>
    metricSpecService.createMetricSpec(metricSpec).map(Created)
  }

  def getMetricSpecById = get("metricspec" :: path[String]) { id: String =>
    metricSpecService.getMetricSpecById(id).map(Ok)
  }

  def getMetricSpecsByModelVersion = get("metricspec" :: "modelversion" :: path[Long]) { modelVersionId: Long =>
    metricSpecService.getMetricSpecsByModelVersion(modelVersionId).map(Ok)
  }

  def getAllMetricSpecs = get("metricspec") {
    metricSpecService.getAllMetricSpecs.map(Ok)
  }
  
  def getMetrics = get("metrics" :: param[Long]("modelVersionId") :: param[Long]("interval") :: params[String]("metrics") :: paramOption[String]("columnIndex")) 
  { (modelVersionId: Long, interval: Long, metrics: Seq[String], columnIndex: Option[String]) =>
    metricStorageService.getMetrics(modelVersionId, interval, metrics, columnIndex).map(Ok)
  }
  
  def getProfiles = get("profiles" :: path[Long] :: path[String]) { (modelVersionId: Long, fieldName: String) =>
    for {
      training <- profileStorageService.getProfile(modelVersionId, fieldName, ProfileSourceKind.Training)
      production <- profileStorageService.getProfile(modelVersionId, fieldName, ProfileSourceKind.Production)
    } yield Ok(ProfileResponse(training, production))
  }

  def endpoints = (healthCheck :+: createMetricSpec :+: getMetricSpecById :+: getAllMetricSpecs :+: getMetricSpecsByModelVersion :+: getMetrics :+: getProfiles) handle {
    case e: io.finch.Error.NotParsed =>
      logger.warn(s"Can't parse json with message: ${e.getMessage()}")
      BadRequest(new RuntimeException(e))
    case e: MetricSpecNotFound =>
      logger.warn(s"Could not find MetriSpec with id ${e.id}")
      NotFound(new RuntimeException(e))
    case NonFatal(e) =>
      logger.error(e.getLocalizedMessage, e)
      InternalServerError(new RuntimeException(e))
  }

  def api: Service[Request, Response] = endpoints.toServiceAs[Application.Json]
}
