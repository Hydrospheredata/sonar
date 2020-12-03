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
  def createBucketIfNotExists(): F[Unit]
}

class S3ParquetSlowStorageService[F[_]: Async](config: Configuration, modelDataService: ModelDataService[F], checkStorageService: CheckStorageService[F]) extends CheckSlowStorageService[F] {

  override def createBucketIfNotExists(): F[Unit] = Async[F].delay {
    val minio = S3Client.fromConfig(config)
    if (!minio.bucketExists(config.storage.bucket))
      minio.makeBucket(config.storage.bucket)
  }

}