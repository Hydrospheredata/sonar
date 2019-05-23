package io.hydrosphere.sonar.endpoints

import java.io.File
import java.util.UUID
import java.util.concurrent.Executors

import cats._
import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._
import com.twitter.finagle.Service
import com.twitter.finagle.http.filter.Cors
import com.twitter.finagle.http.{Request, Response}
import fs2.{text, Stream => Fs2Stream}
import fs2.io.file
import io.circe.{Encoder, Json}
import io.circe.parser.decode
import io.circe.generic.extras.{Configuration => CirceExtraConfiguration}
import io.circe.generic.extras.auto._
import io.finch._
import io.finch.fs2._
import io.finch.circe._
import io.hydrosphere.sonar.BuildInfo
import io.hydrosphere.sonar.services._
import io.hydrosphere.sonar.utils.{CsvRowSizeMismatch, MetricSpecNotFound}
import io.hydrosphere.sonar.Logging
import io.hydrosphere.sonar.terms.{MetricSpec, Profile, ProfileSourceKind}

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

case class ProfileResponse(training: Option[Profile], production: Option[Profile])
case class S3FilePath(path: String)

// TODO: remove after research
case class Test(i: Int, s: String = UUID.randomUUID().toString, r: Double = scala.util.Random.nextDouble())

//noinspection TypeAnnotation
class HttpService[F[_] : Monad : Effect](
  metricSpecService: MetricSpecService[F], 
  metricStorageService: MetricStorageService[F], 
  profileStorageService: ProfileStorageService[F], 
  modelDataService: ModelDataService[F], 
  batchProfileService: BatchProfileService[F, Fs2Stream]
)(implicit cs: ContextShift[F]) extends Logging with Endpoint.Module[F] {

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

  def createMetricSpec = post("monitoring" :: "metricspec" :: stringBody) { body: String =>
    val metricSpec = decode[MetricSpec](body)
    metricSpec match {
      case Left(error) => throw error
      case Right(value) => metricSpecService.createMetricSpec(value).map(Created)
    }
  }

  def getMetricSpecById = get("monitoring" :: "metricspec" :: path[String]) { id: String =>
    metricSpecService.getMetricSpecById(id).map(Ok)
  }

  def getMetricSpecsByModelVersion = get("monitoring" :: "metricspec" :: "modelversion" :: path[Long]) { modelVersionId: Long =>
    metricSpecService.getMetricSpecsByModelVersion(modelVersionId).map(Ok)
  }

  def getAllMetricSpecs = get("monitoring" :: "metricspec") {
    metricSpecService.getAllMetricSpecs.map(Ok)
  }
  
  def deleteMetricSpec = delete("monitoring" :: "metricspec" :: path[String]) { metricSpecId: String =>
    metricSpecService.remove(metricSpecId).map(_ => Ok("ok"))
  }

  def getMetricsAggregation = get("monitoring"
    :: "metrics"
    :: "aggregation"
    :: param[Long]("modelVersionId")
    :: params[String]("metrics")
    :: paramOption[Long]("from")
    :: paramOption[Long]("till")
    :: paramOption[Int]("steps")){
    (modelVersionId:Long, metrics:List[String], from:Option[Long], till:Option[Long], steps:Option[Int]) =>
      metricStorageService.getMetricsAggregationRange(modelVersionId, metrics, from, till, steps.getOrElse(50)).map(Ok)
  }

  def getMetricsRange = get("monitoring"
    :: "metrics"
    :: "range"
    :: param[Long]("modelVersionId")
    :: params[String]("metrics")
    :: param[Long]("from")
    :: param[Long]("till")
    :: paramOption[String]("columnIndex")
    :: paramOption[Int]("health")){
    (modelVersionId:Long, metrics:List[String], from:Long, till:Long, columnIndex: Option[String], health: Option[Int]) =>
      metricStorageService.getMetricsRange(modelVersionId, from, till, metrics, columnIndex, health).map(Ok)
  }
  
  def getMetrics = get("monitoring" :: "metrics" :: param[Long]("modelVersionId") :: param[Long]("interval") :: params[String]("metrics") :: paramOption[String]("columnIndex")) 
  { (modelVersionId: Long, interval: Long, metrics: List[String], columnIndex: Option[String]) =>
    metricStorageService.getMetrics(modelVersionId, interval, metrics, columnIndex).map(Ok)
  }
  
  def getProfiles = get("monitoring" :: "profiles" :: path[Long] :: path[String]) { (modelVersionId: Long, fieldName: String) =>
    for {
      training <- profileStorageService.getProfile(modelVersionId, fieldName, ProfileSourceKind.Training)
      production <- profileStorageService.getProfile(modelVersionId, fieldName, ProfileSourceKind.Production)
    } yield Ok(ProfileResponse(training, production))
  }
  
  def getProfileNames = get("monitoring" :: "fields" :: path[Long]) { modelVersionId: Long =>
    for {
      training <- profileStorageService.getPreprocessedDistinctNames(modelVersionId, ProfileSourceKind.Training)
      production <- profileStorageService.getPreprocessedDistinctNames(modelVersionId, ProfileSourceKind.Production)
    } yield Ok((training ++ production).distinct.sorted)
  }
  
  def batchProfile = post("monitoring" :: "profiles" :: "batch" :: path[Long] :: stringBodyStream[Fs2Stream]) { (modelVersionId: Long, stream: Fs2Stream[F, String]) =>
    for {
      modelVersion <- modelDataService.getModelVersion(modelVersionId)
      tempFile = {
        val f = File.createTempFile("training_data", modelVersionId.toString)
        f.deleteOnExit()
        f.toPath
      }
      executionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))
      _ <- stream
            .through(text.utf8Encode)
            .through(file.writeAll(tempFile, executionContext))
            .compile
            .drain
      _ <- Effect[F].delay(println(tempFile))
      _ <- batchProfileService.batchCsvProcess(tempFile.toString, modelVersion)
    } yield Ok("ok")
  }
  
  def s3BatchProfile = post("monitoring" :: "profiles" :: "batch" :: path[Long] :: jsonBody[S3FilePath]) { (modelVersionId: Long, s3FilePath: S3FilePath) => 
    for {
      modelVersion <- modelDataService.getModelVersion(modelVersionId)
      _ <- batchProfileService.batchCsvProcess(s3FilePath.path, modelVersion)
    } yield Ok("ok")
  }
  
  def getBatchStatus = get("monitoring" :: "profiles" :: "batch" :: path[Long] :: "status") { modelVersionId: Long =>
    batchProfileService.getProcessingStatus(modelVersionId).map(Ok)
  }

  def getBuildInfo = get("monitoring" :: "buildinfo") {
    Ok(BuildInfo.toJson).pure[F]
  }
  
  def endpoints = (getBuildInfo :+: healthCheck :+: createMetricSpec :+: getMetricSpecById :+: getAllMetricSpecs :+: getMetricSpecsByModelVersion :+: getMetricsAggregation :+: getMetricsRange :+: getMetrics :+: getProfiles :+: getProfileNames :+: batchProfile :+: getBatchStatus :+: deleteMetricSpec :+: s3BatchProfile) handle {
    case e: io.finch.Error.NotParsed =>
      logger.warn(s"Can't parse json with message: ${e.getMessage()}")
      BadRequest(new RuntimeException(e))
    case e: MetricSpecNotFound =>
      logger.warn(s"Could not find MetricSpec with id ${e.id}")
      NotFound(new RuntimeException(e))
    case e: CsvRowSizeMismatch =>
      logger.warn("CsvRowSizeMismatch")
      BadRequest(new RuntimeException(e))
    case NonFatal(e) =>
      logger.error(e.getLocalizedMessage, e)
      InternalServerError(new RuntimeException(e))
  }

  def api: Service[Request, Response] = {
    val policy: Cors.Policy = Cors.Policy(
      allowsOrigin = _ => Some("*"),
      allowsMethods = _ => Some(Seq("GET", "POST", "DELETE")),
      allowsHeaders = _ => Some(Seq("*"))
    )

    new Cors.HttpFilter(policy).andThen(endpoints.toServiceAs[Application.Json])
  }
}
