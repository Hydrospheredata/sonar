package io.hydrosphere.sonar.services

import java.io.InputStream
import java.nio.file.Paths
import java.util.concurrent.Executors

import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model._
import enumeratum.{Enum, EnumEntry}
import fs2.io.file
import fs2.{Chunk, Pull, Stream, text}
import io.hydrosphere.serving.manager.data_profile_types.DataProfileType
import io.hydrosphere.serving.manager.grpc.entities.ModelVersion
import io.hydrosphere.sonar.Logging
import io.hydrosphere.sonar.config.Configuration
import io.hydrosphere.sonar.terms.ProfileSourceKind
import io.hydrosphere.sonar.utils.profiles.NumericalProfileUtils
import io.hydrosphere.sonar.utils.{CollectionOps, ContractOps, CsvRowSizeMismatch, ProfileIsAlreadyProcessing}

import scala.collection.immutable
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.util.Try
import scala.util.control.Exception

trait BatchProfileService[F[_], S[_[_], _]] {
  def batchCsvProcess(path: String, modelVersion: ModelVersion): F[Unit]
  
  def getProcessingStatus(modelVersionId: Long): F[BatchProfileService.ProcessingStatus]
}

object BatchProfileService {
  sealed trait ProcessingStatus extends EnumEntry

  object ProcessingStatus extends Enum[ProcessingStatus] {
    val values: immutable.IndexedSeq[ProcessingStatus] = findValues
    case object Success extends ProcessingStatus
    case object Failure extends ProcessingStatus
    case object Processing extends ProcessingStatus
    case object NotRegistered extends ProcessingStatus
  }
}

class BatchProfileServiceInterpreter(config: Configuration, state: Ref[IO, Map[Long, BatchProfileService.ProcessingStatus]], profileStorageService: ProfileStorageService[IO])(implicit cs: ContextShift[IO]) extends BatchProfileService[IO, fs2.Stream] with Logging {
  import BatchProfileService._

//  trait S3Client[F[_]] {
//    private lazy val client = AmazonS3ClientBuilder.defaultClient
//    
//    def objectResource(getObjectRequest: GetObjectRequest)(implicit F: Effect[F]): Resource[F, S3Object] = 
//      Resource.make(F.delay(client.getObject(getObjectRequest)))(obj => F.delay(obj.close()))
//
//    def getObjectContentOrError(getObjectRequest: GetObjectRequest)(implicit F: Effect[F]): F[Either[Throwable, InputStream]] =
//      objectResource(getObjectRequest).use(obj => F.delay(Exception.nonFatalCatch either obj.getObjectContent))
//
//    def getObjectContent(getObjectRequest: GetObjectRequest)(implicit F: Effect[F]): F[InputStream] =
//      F.delay(client.getObject(getObjectRequest).getObjectContent)
//
//    def initiateMultipartUpload(initiateMultipartUploadRequest: InitiateMultipartUploadRequest)(
//      implicit F: Effect[F]): F[InitiateMultipartUploadResult] =
//      F.delay(client.initiateMultipartUpload(initiateMultipartUploadRequest))
//
//    def uploadPart(uploadPartRequest: UploadPartRequest)(implicit F: Effect[F]): F[UploadPartResult] =
//      F.delay(client.uploadPart(uploadPartRequest))
//
//    def completeMultipartUpload(completeMultipartUploadRequest: CompleteMultipartUploadRequest)(
//      implicit F: Effect[F]): F[CompleteMultipartUploadResult] =
//      F.delay(client.completeMultipartUpload(completeMultipartUploadRequest))
//
//    def s3ObjectSummaries(listObjectsV2Request: ListObjectsV2Request)(
//      implicit F: Effect[F]): F[List[S3ObjectSummary]] =
//      F.delay(client.listObjectsV2(listObjectsV2Request).getObjectSummaries.asScala.toList)
//
//    def getObject(objectRequest: GetObjectRequest)(implicit F: Effect[F]): F[S3Object] = {
//      F.delay(client.getObject(objectRequest))
//    }
//  }

  private lazy val client = AmazonS3ClientBuilder.defaultClient


  def readS3FileMultipart[F[_]](
                                 bucket: String,
                                 key: String,
                                 chunkSize: Int,
                                 )(implicit F: Effect[F]): Stream[F, Byte] = {
    def go(offset: Int)(implicit F: Effect[F]): Pull[F, Byte, Unit] =
      Pull
        .acquire[F, S3Object](F.delay(client.getObject(
          new GetObjectRequest(bucket, key).withRange(offset, offset + chunkSize)
        ))) { obj => F.delay(obj.close())
      }
        .flatMap { s3 =>
          Pull.eval(F.delay {
            val is: InputStream = s3.getObjectContent
            val buf = new Array[Byte](chunkSize)
            val len = is.read(buf)
            is.close()
            logger.info(s"Read $len bytes from $offset")
            if (len < chunkSize) None else Some(Chunk.bytes(buf, 0, len))
          })
        } 
        .flatMap {
          case Some(o) => Pull.output(o) >> go(offset + o.size)
          case None => Pull.done
        }

    go(0).stream

  }


  def startProcess(path: String, modelVersion: ModelVersion): IO[Unit] =
    (if (path.startsWith("s3://")) {
      val parts = path.replaceAll("s3://", "").split("/")
      readS3FileMultipart[IO](parts.head, parts.tail.mkString("/"), 2048)
    } else {
      file.readAll[IO](Paths.get(path), ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2)), 2048)
    })
      .through(text.utf8Decode)
      // TODO: parameterize sliding
      .through(text.lines)
      .filter(_.trim.nonEmpty)
      .through(_.map(_.split(",").toVector.map(_.trim)))
      .through {
       csvRows =>
         csvRows.zipWithIndex.mapAccumulate(Option.empty[Vector[String]]) {
           case (None, (headerRow, _)) =>
             (Some(headerRow), Right(Map.empty[String, String]))
           case (h@Some(header), (row, rowIndex)) =>
             if (header.length == row.length) {
               h -> Right(header.zip(row).toMap)
             } else {
               h -> Left(CsvRowSizeMismatch(rowIndex, header.length, row))
             }
         }
           .drop(1)
           .map(_._2)
      }
      .map(_.map(
       _.groupBy({
         case (key, _) =>
           "_\\d+$".r.findFirstIn(key) match {
             case Some(_) => key.split("_").reverse.tail.reverse.mkString("_")
             case None => key
           }
         })
         .mapValues(_.toSeq.sortBy(_._1).map(_._2)))
      )
      .map(_.getOrElse(Map.empty))
      .map(_.filterKeys(key => modelVersion.contract
        .map(ContractOps.extractProfiledFields)
        .getOrElse(Seq.empty)
        .exists(_.name == key)
      ))
      .filter(_.nonEmpty)
      .chunkN(100)
      .flatMap(mapRows => {
        val numericalFields = modelVersion.contract
          .map(ContractOps.extractAllFields)
          .getOrElse(Seq.empty)
          .filter(_.profile == DataProfileType.NUMERICAL)
          .map(_.name)
          .toSet
        logger.info(s"processing ${mapRows.size} csv rows")
        val profiles = for {
          input <- numericalFields
          flat = mapRows.toList.map(_.getOrElse(input, Seq.empty).map(x => Try(x.toDouble).getOrElse(Double.NaN)))
          transposed = CollectionOps.safeTranspose(flat).zipWithIndex
          (column, idx) <- transposed
        } yield NumericalProfileUtils.fromColumn(modelVersion.id, input, idx, column)
        fs2.Stream.eval(profileStorageService.batchSaveProfiles(profiles.toSeq, ProfileSourceKind.Training) *> IO(logger.info("saving batch service profiles was finished")))
      })
      .compile
      .drain
      .flatMap(_ => state.modify(st => (st.updated(modelVersion.id, ProcessingStatus.Success), ())))
      .flatMap(_ => IO(logger.info("batch profiles was computed")))
      .handleErrorWith { e => 
        logger.error("Error in stream", e)
        state.modify(st => (st.updated(modelVersion.id, ProcessingStatus.Failure), ()))
      }

  override def batchCsvProcess(path: String, modelVersion: ModelVersion): IO[Unit] = for {
    st <- state.get
    status = st.getOrElse(modelVersion.id, ProcessingStatus.NotRegistered)
    _ <- status match {
      case _@(ProcessingStatus.Success | ProcessingStatus.Failure | ProcessingStatus.NotRegistered) =>
        state.modify(s => (s.updated(modelVersion.id, ProcessingStatus.Processing), Unit)) *>
          IO.async((cb: Either[Throwable, Unit] => Unit) => {
            startProcess(path, modelVersion).unsafeRunAsyncAndForget()
            cb(Right(Unit))
          })
      case ProcessingStatus.Processing =>
        IO.raiseError(ProfileIsAlreadyProcessing(modelVersion.id))
    }
  } yield Unit

  override def getProcessingStatus(modelVersionId: Long): IO[ProcessingStatus] =
    state.get.map(_.getOrElse(modelVersionId, BatchProfileService.ProcessingStatus.NotRegistered))
}
