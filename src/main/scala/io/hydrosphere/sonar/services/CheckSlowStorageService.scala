package io.hydrosphere.sonar.services

import java.util.Calendar
import cats.effect.{Async, IO, LiftIO, Resource}
import cats.implicits._
import cats.effect.implicits._
import io.hydrosphere.sonar.config.Configuration
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroReadSupport}
import org.apache.parquet.hadoop.util.HadoopInputFile
import tech.allegro.schema.json2avro.converter.JsonAvroConverter

import scala.util.Random
import scala.collection.JavaConverters._

trait CheckSlowStorageService[F[_]] {
  def getChecksByAggregationId(modelVersionId: Long, aggregationId: String): F[Seq[String]]
  
  def getChecksFromAnotherVersion(aggregationId: String, anotherModelVersionId: Long): F[Seq[String]]
  
  def getCheckSubsample(modelVersionId: Long, size: Int): F[Seq[String]]

  def createBucketIfNotExists(): F[Unit]
}

class S3ParquetSlowStorageService[F[_]: Async](config: Configuration, modelDataService: ModelDataService[F], checkStorageService: CheckStorageService[F]) extends CheckSlowStorageService[F] {

  override def createBucketIfNotExists(): F[Unit] = Async[F].delay {
    val minio = S3Client.fromConfig(config)
    val exists = minio.bucketExists(config.storage.bucket)
    if (!exists && config.storage.createBucket)
      minio.makeBucket(config.storage.bucket)
  }

  override def getCheckSubsample(modelVersionId: Long, size: Int): F[Seq[String]] = for {
      model <- modelDataService.getModelVersion(modelVersionId)
      batchSize = model.monitoringConfiguration.map(_.batchSize).getOrElse(config.monitoring.batchSize)

      allFilePaths <- Async[F].delay {
      val minio = S3Client.fromConfig(config)
      val iter = minio.listObjects(config.storage.bucket, s"${model.model.map(_.name).getOrElse("_unknown")}/${model.version}")
      iter.asScala.map(_.get.objectName()).toSeq
    }
    (paths, actualRowsPerFile) = {

      val actualSize = if (size < 1) 1 else if (size > batchSize * 10) batchSize * 10 else size
      if (allFilePaths.isEmpty) {
        (Seq.empty, 0)
      } else {
        val rowsPerFile = actualSize / allFilePaths.size
        if (rowsPerFile >= batchSize) {
          (allFilePaths, batchSize)
        } else if (rowsPerFile <= batchSize / 2) { // TODO: need better number (configurable?)
          (Random.shuffle(allFilePaths).take(actualSize / (batchSize / 2)), batchSize / 2)
        } else {
          (allFilePaths, rowsPerFile)
        }  
      }
    }
    result <- getChecks(modelVersionId, paths.map(s"s3a://${config.storage.bucket}/" + _), actualRowsPerFile)
  } yield Random.shuffle(result).take(size)

  override def getChecksFromAnotherVersion(aggregationId: String, anotherModelVersionId: Long): F[Seq[String]] = ???

  override def getChecksByAggregationId(modelVersionId: Long, aggregationId: String): F[Seq[String]] = for {
    maybeAggregate <- checkStorageService.getAggregateById(modelVersionId, aggregationId)
    model <- modelDataService.getModelVersion(modelVersionId)
    aggregations = maybeAggregate.toSeq
    paths = aggregations.flatMap(agg => {
      val dates = Seq(new Calendar.Builder().setInstant(agg.startTimestamp * 1000).build(), new Calendar.Builder().setInstant(agg.endTimestamp * 1000).build())
      dates.map(date => s"s3a://${config.storage.bucket}/${model.model.map(_.name).getOrElse("_unknown")}/${model.version}/_hs_year=${date.get(Calendar.YEAR)}/_hs_month=${date.get(Calendar.MONTH) + 1}/_hs_day=${date.get(Calendar.DAY_OF_MONTH)}/${agg.id}.parquet").distinct
    })
    result <- getChecks(modelVersionId, paths)
  } yield result
  
  private def getChecks(modelVersionId: Long, paths: Seq[String], max: Int = Int.MaxValue): F[Seq[String]] = {
    // TODO: this is a copypaste
    val conf = new HadoopConfiguration()
    if (config.storage.accessKey.isDefined)
      conf.set("fs.s3a.access.key", config.storage.accessKey.get)
    if (config.storage.secretKey.isDefined)
      conf.set("fs.s3a.secret.key", config.storage.secretKey.get)
    if (config.storage.endpoint.isDefined)
      conf.set("fs.s3a.endpoint", config.storage.endpoint.get)
    if (config.storage.pathStyleAccess.isDefined)
      conf.set("fs.s3a.path.style.access", config.storage.pathStyleAccess.get)
    if (config.storage.s3Impl.isDefined)
      conf.set("fs.s3a.impl", config.storage.s3Impl.get)

    val converter = new JsonAvroConverter()
    
    def fileResource(filePath: Path) = Resource.make(IO(AvroParquetReader.builder[GenericData.Record](HadoopInputFile.fromPath(filePath, conf)).withConf(conf).build()))(reader => IO(reader.close()))
    
    val tasks: IO[Seq[Seq[String]]] = paths.toList.traverse((path: String) => fileResource(new Path(path)).use { reader =>
      IO {
        Iterator
          .continually(reader.read())
          .takeWhile(_ != null)
          .take(max)
          .map(converter.convertToJson)
          .map(new String(_))
          .toSeq
      }
    })
    
    LiftIO[F].liftIO(tasks).map(_.flatten)
  } 
    
//    Async[F].delay {
//    // TODO: this is a copypaste
//    val conf = new HadoopConfiguration()
//    if (config.storage.accessKey.isDefined)
//      conf.set("fs.s3a.access.key", config.storage.accessKey.get)
//    if (config.storage.secretKey.isDefined)
//      conf.set("fs.s3a.secret.key", config.storage.secretKey.get)
//    if (config.storage.endpoint.isDefined)
//      conf.set("fs.s3a.endpoint", config.storage.endpoint.get)
//    if (config.storage.pathStyleAccess.isDefined)
//      conf.set("fs.s3a.path.style.access", config.storage.pathStyleAccess.get)
//    if (config.storage.s3Impl.isDefined)
//      conf.set("fs.s3a.impl", config.storage.s3Impl.get)
//
//    val converter = new JsonAvroConverter()
////    val a: F[Int] = LiftIO[F].liftIO(IO.pure(1))
//    paths.flatMap(path => {
//      val avroParquetReader = AvroParquetReader
//        .builder[GenericData.Record](HadoopInputFile.fromPath(new Path(path), conf))
//        .withConf(conf)
//        .build()
//      val r = Iterator
//        .continually(avroParquetReader.read())
//        .takeWhile(_ != null)
//        .take(max)
//        .map(converter.convertToJson)
//        .map(new String(_))
//        .toSeq
//      avroParquetReader.close()
//      r
//    })
//  }
}