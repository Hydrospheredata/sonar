package io.hydrosphere.sonar.endpoints

import java.io.{File, FileOutputStream, PrintWriter}
import java.util.UUID
import java.util.concurrent.Executors

import cats._
import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._
import com.twitter.finagle.http.Status
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.http.filter.LogFormatter
import com.twitter.util.{Duration, Future, Return, Stopwatch, Throw}
//import com.twitter.finagle.filter.LoggingFilter
import com.twitter.finagle.http.filter.{CommonLogFormatter, Cors, LoggingFilter}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.logging.Logger
import fs2.{text, Stream => Fs2Stream}
import fs2.io.file
import io.circe.{Encoder, Json}
import io.circe.parser.{decode, parse}
import io.circe.generic.extras.{Configuration => CirceExtraConfiguration}
import io.circe.generic.extras.auto._
import io.finch._
import io.finch.fs2._
import io.finch.circe._
import io.hydrosphere.sonar.BuildInfo
import io.hydrosphere.sonar.services._
import io.hydrosphere.sonar.utils.{CsvRowSizeMismatch, MetricSpecNotFound}
import io.hydrosphere.sonar.Logging
import io.hydrosphere.sonar.terms.{Profile, ProfileSourceKind}
import com.twitter.finagle.http.filter.{CommonLogFormatter => FinagleCommonLogFormatter}
import com.twitter.finagle.filter.{LogFormatter => FinagleLogFormatter}


import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

case class ProfileResponse(training: Option[Profile], production: Option[Profile])
case class S3FilePath(path: String)

abstract class RequestLoggingFilter[REQ <: Request](val formatter: FinagleLogFormatter[REQ, Response])
  extends SimpleFilter[REQ, Response] with Logging {

  def apply(request: REQ, service: Service[REQ, Response]): Future[Response] = {
    val elapsed = Stopwatch.start()
    val future = service(request)
    future.respond {
      case Return(reply) => logSuccess(elapsed(), request, reply)
      case Throw(throwable) => logException(elapsed(), request, throwable)
    }
    future
  }

  def logSuccess(replyTime: Duration, request: REQ, reply: Response) {
    val line = formatter.format(request, reply, replyTime)
    logger.info(line)
    logger.info(request.headerMap.mkString("\n"))
    logger.info(s"is chunked: ${request.isChunked}")
  }

  def logException(duration: Duration, request: REQ, throwable: Throwable) {
    val response = Response(request.version, Status.InternalServerError)
    val line = formatter.format(request, response, duration)
    logger.info(line)
    logger.info(request.headerMap.mkString("\n"))
    logger.info(s"is chunked: ${request.isChunked}")
  }
}

object RequestLoggingFilter extends RequestLoggingFilter[Request](new FinagleCommonLogFormatter)

//noinspection TypeAnnotation
class HttpService[F[_] : Monad : Effect](
                                          metricSpecService: MetricSpecService[F],
                                          profileStorageService: ProfileStorageService[F],
                                          modelDataService: ModelDataService[F],
                                          batchProfileService: TrainingProfileService[F, Fs2Stream],
                                          checkStorageService: CheckStorageService[F]
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
  
  def fileBatchProfile = post("monitoring" :: "profiles" :: "batch" :: path[Long] :: stringBody) { (modelVersionId: Long, contents: String) => 
    for {
      _ <- Effect[F].delay(logger.info("file"))
      modelVersion <- modelDataService.getModelVersion(modelVersionId)
      tempFile = {
        val f = File.createTempFile("training_data", modelVersionId.toString)
        f.deleteOnExit()
        f
      }
      _ <- Effect[F].delay {
        new PrintWriter(tempFile) {
          // todo: ooph, rewrite
          try {
            write(contents)
          } finally {
            close()
          }
        }
      }
      _ <- batchProfileService.batchCsvProcess(tempFile.toString, modelVersion)
    } yield Ok("ok")
  }
  
  def batchProfile = post("monitoring" :: "profiles" :: "batch" :: path[Long] :: stringBodyStream[Fs2Stream]) { (modelVersionId: Long, stream: Fs2Stream[F, String]) =>
    for {
      _ <- Effect[F].delay(logger.info("stream"))
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
  
  def s3BatchProfile = post("monitoring" :: "profiles" :: "batch" :: path[Long] :: "s3" :: jsonBody[S3FilePath]) { (modelVersionId: Long, s3FilePath: S3FilePath) => 
    for {
      _ <- Effect[F].delay(logger.info("s3"))
      modelVersion <- modelDataService.getModelVersion(modelVersionId)
      _ <- batchProfileService.batchCsvProcess(s3FilePath.path, modelVersion)
    } yield Ok("ok")
  }
  
  def getBatchStatus = get("monitoring" :: "profiles" :: "batch" :: path[Long] :: "status") { modelVersionId: Long =>
    batchProfileService.getProcessingStatus(modelVersionId).map(Ok)
  }

  def getBuildInfo = get("monitoring" :: "buildinfo") {
    Ok(BuildInfo.value).pure[F]
  }
  
  def getChecks = get("monitoring" :: "checks" :: path[Long] :: param[String]("from") :: param[String]("to")) { (modelVersionId: Long, from: String, to: String) =>
    checkStorageService.getChecks(modelVersionId, from, to).map { jsonStrings => // TODO: ooph, dirty hacks
      jsonStrings.map(jsonString =>
        parse(jsonString) match {
          case Left(value) => Json.Null
          case Right(value) => value
        }
      )
    }.map(Ok)
  }
  
  def getChecksWithOffset = get("monitoring" :: "checks" :: "all" :: path[Long] :: param[Int]("limit") :: param[Int]("offset")) { (modelVersionId: Long, limit: Int, offset: Int) =>
    checkStorageService.getChecks(modelVersionId, limit, offset).map { jsonStrings => // TODO: ooph, dirty hacks
      jsonStrings.map(jsonString =>
        parse(jsonString) match {
          case Left(value) => Json.Null
          case Right(value) => value
        }
      )
    }.map(Ok)
  }

  def getCheckAggregates = get("monitoring" :: "checks" :: "aggregates" :: path[Long] :: param[Int]("limit") :: param[Int]("offset")) { (modelVersionId: Long, limit: Int, offset: Int) =>
    val program = for {
      count <- checkStorageService.getAggregateCount(modelVersionId)
      jsonStrings <- checkStorageService.getAggregates(modelVersionId, limit, offset)
      jsons = jsonStrings.map((jsonString: String) =>
        parse(jsonString) match {
          case Left(value) => Json.Null
          case Right(value) => value
        }
      )
    } yield Json.obj("count" -> Json.fromLong(count), "results" -> Json.arr(jsons: _*))
    
    program.map(Ok _)
  }
  
  def getCheckById = get("monitoring" :: "checks" :: path[String]) { (id: String) =>
    checkStorageService.getCheckById(id).map { maybeString => maybeString.map { jsonString =>
      parse(jsonString) match {
        case Left(value) => Json.Null
        case Right(value) => value
      }
    }
    }.map(Ok)
  }

  def endpoints = (getChecks :+: getCheckById :+: getCheckAggregates :+: getBuildInfo :+: healthCheck :+: getProfiles :+: getProfileNames :+: batchProfile :+: getBatchStatus :+: fileBatchProfile :+: s3BatchProfile :+: getChecksWithOffset) handle {
    case e: io.finch.Error.NotParsed =>
      logger.warn(s"Can't parse json with message: ${e.getMessage()}")
      BadRequest(new RuntimeException(e))
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

    new Cors.HttpFilter(policy)./*andThen(RequestLoggingFilter).*/andThen(endpoints.toServiceAs[Application.Json])
  }
}
