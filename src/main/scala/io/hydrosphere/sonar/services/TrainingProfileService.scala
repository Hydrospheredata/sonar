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
import io.circe.generic.JsonCodec
import io.hydrosphere.serving.proto.contract.types.DataProfileType
import io.hydrosphere.serving.proto.manager.entities.ModelVersion
import io.hydrosphere.sonar.Logging
import io.hydrosphere.sonar.config.Configuration
import io.hydrosphere.sonar.terms.ProfileSourceKind
import io.hydrosphere.sonar.utils.SignatureOps._
import io.hydrosphere.sonar.utils.mongo.ObservableExt._
import io.hydrosphere.sonar.utils.profiles.NumericalProfileUtils
import io.hydrosphere.sonar.utils.{CollectionOps, CsvRowSizeMismatch, ProfileIsAlreadyProcessing}
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.{Document, MongoClient}

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.util.Try

trait TrainingProfileService[F[_], S[_[_], _]] {
  def batchCsvProcess(path: String, modelVersion: ModelVersion): F[Unit]
  
  def getProcessingStatus(modelVersionId: Long): F[TrainingProfileService.ProcessingStatus]

  def getTrainingData(modelVersionId: Long): F[Seq[String]]
}

object TrainingProfileService {
  @JsonCodec
  sealed trait ProcessingStatus extends EnumEntry

  object ProcessingStatus extends Enum[ProcessingStatus] {
    val values: immutable.IndexedSeq[ProcessingStatus] = findValues
    case object Success extends ProcessingStatus
    case object Failure extends ProcessingStatus
    case object Processing extends ProcessingStatus
    case object NotRegistered extends ProcessingStatus
  }
}

class TrainingProfileServiceInterpreter(config: Configuration, state: Ref[IO, Map[Long, TrainingProfileService.ProcessingStatus]], profileStorageService: ProfileStorageService[IO], mongoClient: MongoClient, autoODService: AutoODService[IO])(implicit cs: ContextShift[IO]) extends TrainingProfileService[IO, fs2.Stream] with Logging {

  import TrainingProfileService._

  // TODO(Bulat): extract dependencies
  private lazy val client = AmazonS3ClientBuilder.defaultClient
  private lazy val minio = S3Client.fromConfig(config)
  private val db = mongoClient.getDatabase(config.mongo.database)
  private val trainingDataCollection = db.getCollection("training_data")

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

  object TrainingDataDocument {
    final val ModelVersionId = "model-version-id"
    final val PathToData = "path-to-data"

    def apply(path: String, modelVersion: ModelVersion): Document = {
      Document(
        ModelVersionId -> modelVersion.id,
        PathToData -> path,
        "model-name" -> modelVersion.name,
        "model-version" -> modelVersion.version,
      )
    }
  }

  def handleS3Object(path: String, modelVersion: ModelVersion): Stream[IO, Byte] = {
    val parts = path.replaceAll("s3://", "").split("/")
    val document = TrainingDataDocument(path, modelVersion)
    fs2.Stream.eval(IO.suspend(trainingDataCollection.insertOne(document).toF[IO])) >>
      fs2.Stream.eval(IO.suspend(autoODService.launchAutoOD(modelVersion.id, path))) >>
      readS3FileMultipart[IO](parts.head, parts.tail.mkString("/"), 2048)
  }

  def calculateTrainingDataKey(path: String, modelVersion: ModelVersion): String = {
    val filename = Paths.get(path).getFileName.toString
    s"training-data/${modelVersion.id}/$filename.csv"
  }

  def handleLocalFile(path: String, modelVersion: ModelVersion): Stream[IO, Byte] = {
    val objectKey = calculateTrainingDataKey(path, modelVersion)
    val fullS3Path = s"s3://${config.storage.bucket}/$objectKey"
    val document = TrainingDataDocument(fullS3Path, modelVersion)
    val s3CopyProgram = IO {
      minio.putObject(config.storage.bucket, objectKey, path)
    }
    fs2.Stream.eval(s3CopyProgram) >>
      fs2.Stream.eval(IO.suspend(trainingDataCollection.insertOne(document).toF[IO])) >>
      fs2.Stream.eval(IO.suspend(autoODService.launchAutoOD(modelVersion.id, fullS3Path))) >>
      file.readAll[IO](Paths.get(path), ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2)), 2048)
  }

  def processFile(path: String, modelVersion: ModelVersion): Stream[IO, Byte] = {
    if (path.startsWith("s3://")) {
      handleS3Object(path, modelVersion)
    } else {
      handleLocalFile(path, modelVersion)
    }
  }

  def startProcess(path: String, modelVersion: ModelVersion): IO[Unit] =
    processFile(path, modelVersion)
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
      .map(_.filterKeys(key => modelVersion.signature
        .map(_.extractProfiledFields)
        .getOrElse(Seq.empty)
        .exists(_.name == key)
      ))
      .filter(_.nonEmpty)
      .chunkN(100)
      .flatMap(mapRows => {
        val numericalFields = modelVersion.signature
          .map(_.extractAllFields)
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
    state.get.map(_.getOrElse(modelVersionId, TrainingProfileService.ProcessingStatus.NotRegistered))

  override def getTrainingData(modelVersionId: Long): IO[Seq[String]] = {
    for {
      data <- IO.suspend(trainingDataCollection.find(equal(TrainingDataDocument.ModelVersionId, modelVersionId)).toF[IO])
    } yield data.map(x => x.getString(TrainingDataDocument.PathToData))
  }
}