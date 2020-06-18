package io.hydrosphere.sonar.services

import java.time.Instant

import cats.Applicative
import cats.data.NonEmptyList
import cats.effect.Async
import cats.implicits._
import com.twitter.finagle.http.{Method, Request, Response}
import com.twitter.finagle.{Http, Service}
import com.twitter.io.Buf
import eu.timepit.refined._
import eu.timepit.refined.string._
import io.circe.generic.auto._
import io.circe.refined._
import io.circe.syntax._
import io.hydrosphere.serving.manager.grpc.entities.ModelVersion
import io.hydrosphere.serving.monitoring.api.ExecutionInformation
import io.hydrosphere.sonar.{Logging, URIString, URLString}
import io.hydrosphere.sonar.terms.Check
import io.hydrosphere.sonar.utils.FutureOps._

trait AlertService[F[_]] {
  def sendChecks(
    executionInformation: ExecutionInformation,
    modelVersion: ModelVersion,
    profileChecks: Map[String, Seq[Check]],
    metricChecks: Map[String, Check]
  ): F[Unit]
}

class NoopAlertService[F[_]](implicit F: Applicative[F]) extends AlertService[F] {
  override def sendChecks(
    executionInformation: ExecutionInformation,
    modelVersion: ModelVersion,
    profileChecks: Map[String, Seq[Check]],
    metricChecks: Map[String, Check]
  ): F[Unit] = F.unit
}

class PrometheusAMService[F[_]](
  amUrl: URIString,
  baseUrl: URLString)(
  implicit F: Async[F]) extends AlertService[F] with Logging {
  val alertmanagerClient: Service[Seq[AMAlert], Response] = Http.client
    .withSessionQualifier.noFailFast.newService(amUrl.toString())
    .map[Seq[AMAlert]] { alerts =>
      val content = Buf.Utf8.apply(alerts.asJson.noSpaces)
      val ir = Request(Method.Post, "/api/v2/alerts")
        .content(content)
      ir.contentType = "application/json"
      ir.host = amUrl.toString()
      ir
    }

  override def sendChecks(
    executionInformation: ExecutionInformation,
    modelVersion: ModelVersion,
    profileChecks: Map[String, Seq[Check]],
    metricChecks: Map[String, Check]
  ): F[Unit] = {
    val failedProfileChecks = profileChecks.toList.flatMap{ case(name, checks) =>
      checks.collect{
        case c @ Check(false, _, _, _, _) =>
          name -> c
      }
    }
    val failedMetricChecks = metricChecks.toList.collect {
      case (name, c @ Check(false, _, _, _, _)) =>
        name -> c
    }
    val failedChecks = failedMetricChecks ++ failedProfileChecks

    NonEmptyList.fromList(failedChecks) match {
      case Some(nel) => sendFailedChecks(executionInformation, modelVersion, nel)
      case None => F.unit
    }
  }

  def sendFailedChecks(
    exInfo: ExecutionInformation,
    modelVersion: ModelVersion,
    failedChecks: NonEmptyList[(String, Check)]
  ): F[Unit] = {
    val alerts = failedChecks.map { case (fieldName, check) =>
      checkToAMAlert(exInfo, modelVersion, fieldName, check)
    }
    for {
      response <- F.defer(alertmanagerClient(alerts.toList).liftToAsync[F])
      _ <- response.statusCode match {
        case 200 => F.unit
        case x =>
          val errorMsg = Buf.Utf8.unapply(response.content).getOrElse("<no error message>")
          val ex = AMException(s"Can't send metrics to $amUrl. Response $x: $errorMsg")
          logger.debug(s"Can't send alerts to AlertManager", ex)
          F.raiseError[Unit](ex)
      }
    } yield ()
  }

  def checkToAMAlert(
    exInfo: ExecutionInformation,
    modelVersion: ModelVersion,
    fieldName: String,
    check: Check
  ): AMAlert = {
    val labels: Map[String, String] = List(
      exInfo.metadata.toList.flatMap { meta =>
        List(
          "modelVersionId" -> meta.modelVersionId.toString,
          "modelVersionIncrement" -> meta.modelVersion.toString,
          "modelName" -> meta.modelName,
          "requestId" -> meta.requestId
        ) ++ meta.appInfo.toList.flatMap { appInfo =>
          List(
            "applicationId" -> appInfo.applicationId.toString,
            "applicationStageId" -> appInfo.stageId
          )
        }
      },
      List("fieldName" -> fieldName),
      check.metricSpecId.toList.map(x => "metricSpecId" -> x)
    ).flatten.toMap

    val annotations: Map[String, String] = List(
      Some("checkValue" -> check.value.toString),
      Some("checkThreshold" -> check.threshold.toString),
      Some("checkDescription" -> check.description),
    ).flatten.toMap

    AMAlert(
      startsAt = Instant.now(),
      generatorUrl = generatorUrl(modelVersion),
      annotations = annotations,
      labels = labels
    )
  }

  def generatorUrl(modelVersion: ModelVersion): URLString = {
    refineV[Url](s"$baseUrl/models/${modelVersion.model.map(_.id).getOrElse(1)}/${modelVersion.id}/monitoring/dashboard")
      .toOption
      .getOrElse(baseUrl)
  }
}

case class AMException(msg: String) extends Throwable {
  override def getMessage: String = msg
}

/**
 *
 * @param startsAt Alert start time
 * @param endsAt Alert end time. If empty, then configured AM alert timeout applies
 * @param generatorUrl Callback url for the user
 * @param annotations Annotations for the event. Actual event payload.
 * @param labels identifiers for AM to manage similar alerts
 */
case class AMAlert(
  startsAt: Instant,
  endsAt: Option[Instant] = None,
  generatorUrl: URLString,
  annotations: Map[String, String] = Map.empty,
  labels: Map[String, String] = Map.empty)