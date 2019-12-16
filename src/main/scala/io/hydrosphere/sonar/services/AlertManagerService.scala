//package io.hydrosphere.sonar.services
//
//import java.time.Instant
//
//import cats.{Applicative, Monad}
//import cats.implicits._
//import cats.effect.Async
//import com.twitter.finagle.Http
//import com.twitter.finagle.http.{Method, Request}
//import com.twitter.io.Buf
//import io.hydrosphere.sonar.terms.Metric
//import io.circe.syntax._
//import io.circe.generic.auto._
//import io.hydrosphere.sonar.Logging
//import io.hydrosphere.sonar.utils.FutureOps._
//
//trait AlertManagerService[F[_]] {
//  def sendMetric(metrics: Seq[Metric]): F[Unit]
//}
//
//
//object AlertManagerService extends Logging {
//
//  type GeneratorUrl[F[_]] = Metric => F[String]
//
//  case class AMAlert(
//                      startsAt: Instant,  // Alert start time
//                      endsAt: Option[Instant] = None,  // Alert end time. If empty, then configured AM alert timeout applies
//                      generatorUrl: String,  // Callback url for the user
//                      annotations: Map[String, String] = Map.empty, // ???
//                      labels: Map[String, String] = Map.empty)  // identifiers for AM to manage similar alerts
//
//  case class AMException(msg: String) extends Throwable {
//    override def getMessage: String = msg
//  }
//
//  def noop[F[_]]()(implicit F: Applicative[F]): AlertManagerService[F] = new AlertManagerService[F] {
//    override def sendMetric(metrics: Seq[Metric]): F[Unit] = F.unit
//  }
//
//  def metricGenUrl[F[_]](baseUrl: String, modelDataService: ModelDataService[F])(implicit F: Monad[F]): GeneratorUrl[F] = (m: Metric) => {
//    for {
//      mv <- modelDataService.getModelVersion(m.labels.modelVersionId)
//    } yield s"$baseUrl/models/${mv.model.map(_.id).getOrElse(1)}/${m.labels.modelVersionId}/monitoring/dashboard"
//  }
//
//  def prometheus[F[_]](amUrl: String, baseUrl: String, modelDataService: ModelDataService[F])(implicit F: Async[F]): AlertManagerService[F] = {
//    val service = Http.client
//      .withSessionQualifier.noFailFast
//      .newService(amUrl)
//    val generatorUrl = metricGenUrl(baseUrl, modelDataService)
//    new AlertManagerService[F] {
//      override def sendMetric(metrics: Seq[Metric]): F[Unit] = {
//        for {
//          alerts <- metrics
//            .toList
//            .filterNot(_.health.getOrElse(true))
//            .traverse { m =>
//              generatorUrl(m).map { url =>
////                {
////                  "labels": {
////                    "alertname": "serving-sonar",
////                    "metricSpecId": "2",
////                    "modelVersionId": "2"
////                  },
////                  "annotations": {
////                    "metricValue": "100000",
////                    "summary": "Threshold violation. KS test result (10000) > 50"
////                  },
////                  "generatorUrl": "https://www.getpostman.com/downloads/"
////                }
//                AMAlert(
//                  startsAt = Instant.ofEpochMilli(m.timestamp),
//                  generatorUrl = url,
//                  labels = Map(
//                    "alertname" -> "serving-sonar",
//                    "metricName" -> m.name,
//                    "modelVersionId" -> m.labels.modelVersionId.toString,
//                    "metricSpecId" -> m.labels.metricSpecId
//                  ),
//                  annotations = Map(
//                    "metricValue" -> m.value.toString,
//                    "summary" -> s"Metric ${m.name}(=${m.value}) detected data anomaly"
//                  )
//                )
//              }
//            }
//          content = Buf.Utf8.apply(alerts.asJson.noSpaces)
//          request = {
//            val ir = Request(Method.Post, "/api/v2/alerts")
//              .content(content)
//            ir.contentType = "application/json"
//            ir.host = amUrl
//            ir
//          }
//          response <- F.defer {
//            service(request).liftToAsync[F]
//          }
//          _ <- response.statusCode match {
//            case 200 => F.unit
//            case x => F.raiseError[Unit](AMException(s"Can't send metrics to ${amUrl}. Response ${x}: ${Buf.Utf8.unapply(response.content).getOrElse("")}"))
//          }
//        } yield ()
//      }
//    }
//  }
//}