package io.hydrosphere.sonar.services

import java.nio.file.{Path, Paths}
import java.util.concurrent.Executors

import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import enumeratum.{Enum, EnumEntry}
import fs2.io.file
import fs2.text
import io.hydrosphere.serving.manager.data_profile_types.DataProfileType
import io.hydrosphere.serving.manager.grpc.entities.ModelVersion
import io.hydrosphere.sonar.Logging
import io.hydrosphere.sonar.config.Configuration
import io.hydrosphere.sonar.terms.ProfileSourceKind
import io.hydrosphere.sonar.utils.profiles.NumericalProfileUtils
import io.hydrosphere.sonar.utils.{CollectionOps, CsvRowSizeMismatch, ProfileIsAlreadyProcessing}

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.util.Try

trait BatchProfileService[F[_], S[_[_], _]] {
//  def batchCsvProcess(stream: S[F, String], modelVersion: ModelVersion): F[Unit]
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
  
  def startProcess(path: Path, modelVersion: ModelVersion): IO[Unit] =
    file.readAll[IO](path, ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2)), 2048)
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
      .map(_.filterKeys(key => modelVersion.dataTypes.contains(key)))
      .filter(_.nonEmpty)
      .sliding(1000)
      .flatMap(mapRows => {
        val numericalInputs = modelVersion.dataTypes.filter({ case (_, tpe) => tpe == DataProfileType.NUMERICAL }).keys.toSet
        val profiles = for {
          input <- numericalInputs
          flat = mapRows.map(_.getOrElse(input, Seq.empty).map(x => Try(x.toDouble).getOrElse(Double.NaN)))
          transposed = CollectionOps.safeTranspose(flat).zipWithIndex
          (column, idx) <- transposed
        } yield NumericalProfileUtils.fromColumn(modelVersion.id, input, idx, column)
        fs2.Stream.eval(profileStorageService.batchSaveProfiles(profiles.toSeq, ProfileSourceKind.Training))
      })
      .compile
      .drain
      .flatMap(_ => state.modify(st => (st.updated(modelVersion.id, ProcessingStatus.Success), ())))
      .handleErrorWith { e => 
        logger.error("Error in stream", e)
        state.modify(st => (st.updated(modelVersion.id, ProcessingStatus.Failure), ()))
      }
  
//  override def batchCsvProcess(stream: fs2.Stream[IO, String], modelVersion: ModelVersion): IO[Unit] = for {
//    st <- state.get
//    status = st.getOrElse(modelVersion.id, ProcessingStatus.NotRegistered)
//    _ <- status match {
//      case _ @ (ProcessingStatus.Success | ProcessingStatus.Failure | ProcessingStatus.NotRegistered) =>
//        state.modify(s => (s.updated(modelVersion.id, ProcessingStatus.Processing), Unit)) *> 
//          IO.async((cb: Either[Throwable, Unit] => Unit) => {
//            startProcess(stream, modelVersion).unsafeRunAsyncAndForget()
//            cb(Right(Unit))
//          })
//      case ProcessingStatus.Processing => 
//        IO.raiseError(ProfileIsAlreadyProcessing(modelVersion.id))
//    }
//  } yield Unit


  override def batchCsvProcess(path: String, modelVersion: ModelVersion): IO[Unit] = for {
    st <- state.get
    status = st.getOrElse(modelVersion.id, ProcessingStatus.NotRegistered)
    _ <- status match {
      case _@(ProcessingStatus.Success | ProcessingStatus.Failure | ProcessingStatus.NotRegistered) =>
        state.modify(s => (s.updated(modelVersion.id, ProcessingStatus.Processing), Unit)) *>
          IO.async((cb: Either[Throwable, Unit] => Unit) => {
            startProcess(Paths.get(path), modelVersion).unsafeRunAsyncAndForget()
            cb(Right(Unit))
          })
      case ProcessingStatus.Processing =>
        IO.raiseError(ProfileIsAlreadyProcessing(modelVersion.id))
    }
  } yield Unit

  override def getProcessingStatus(modelVersionId: Long): IO[ProcessingStatus] =
    state.get.map(_.getOrElse(modelVersionId, BatchProfileService.ProcessingStatus.NotRegistered))
}
