package io.hydrosphere.sonar.services

import java.util.Calendar

import cats.effect.Async
import cats.implicits._
import io.hydrosphere.sonar.config.Configuration
import io.hydrosphere.sonar.terms.AggregationMetadata
import org.bson.types.ObjectId
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import io.hydrosphere.sonar.utils.ContractOps._
import io.hydrosphere.sonar.utils.ParquetHelper
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import tech.allegro.schema.json2avro.converter.JsonAvroConverter

import scala.util.Random

trait CheckSlowStorageService[F[_]] {
  def getChecksByAggregationId(modelVersionId: Long, aggregationId: String): F[Seq[String]]
  
  def getChecksFromAnotherVersion(aggregationId: String, anotherModelVersionId: Long): F[Seq[String]]
  
  def getCheckSubsample(modelVersionId: Long, size: Int): F[Seq[String]]
}

class S3ParquetSlowStorageService[F[_]: Async](config: Configuration, modelDataService: ModelDataService[F], checkStorageService: CheckStorageService[F]) extends CheckSlowStorageService[F] {


  override def getCheckSubsample(modelVersionId: Long, size: Int): F[Seq[String]] = for {
    aggregates <- checkStorageService.getAggregatesForSubsample(modelVersionId, size)
    result <- getChecks(modelVersionId, aggregates)
  } yield Random.shuffle(result).take(size)

  override def getChecksFromAnotherVersion(aggregationId: String, anotherModelVersionId: Long): F[Seq[String]] = ???

  override def getChecksByAggregationId(modelVersionId: Long, aggregationId: String): F[Seq[String]] = for {
//    maybeTimestamps <- checkStorageService.getAggregateDateBounds(modelVersionId, aggregationId)
//    timestamps = maybeTimestamps.getOrElse((0L, 0L)) // TODO: do nothing if None
    maybeAggregate <- checkStorageService.getAggregateById(modelVersionId, aggregationId)
    result <- getChecks(modelVersionId, maybeAggregate.toSeq)
  } yield result
  
  private def getChecks(modelVersionId: Long, aggregations: Seq[AggregationMetadata]): F[Seq[String]] = for {
    model <- modelDataService.getModelVersion(modelVersionId)
    paths = aggregations.flatMap(agg => {
      val dates = Seq(new Calendar.Builder().setInstant(agg.startTimestamp * 1000).build(), new Calendar.Builder().setInstant(agg.endTimestamp * 1000).build())
      dates.map(date => s"s3a://${config.storage.bucket}/${model.model.map(_.name).getOrElse("_unknown")}/${model.version}/_hs_year=${date.get(Calendar.YEAR)}/_hs_month=${date.get(Calendar.MONTH) + 1}/_hs_day=${date.get(Calendar.DAY_OF_MONTH)}/${agg.id}.parquet").distinct
    })
//    dates = Seq( new Calendar.Builder().setInstant(startTimestampSeconds * 1000).build(), new Calendar.Builder().setInstant(endTimestampSeconds * 1000).build())
//    paths = dates.map(date => s"s3a://${config.storage.bucket}/${model.model.map(_.name).getOrElse("_unknown")}/${model.version}/_hs_year=${date.get(Calendar.YEAR)}/_hs_month=${date.get(Calendar.MONTH) + 1}/_hs_day=${date.get(Calendar.DAY_OF_MONTH)}/$aggregationId.parquet").distinct
    result <- Async[F].delay {
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
      //HadoopInputFile.fromPath(new Path(paths.head), conf)
      paths.flatMap(path => {
        val avroParquetReader = AvroParquetReader.builder[GenericData.Record](HadoopInputFile.fromPath(new Path(path), conf)).withConf(conf).build()
        Iterator.continually(avroParquetReader.read()).takeWhile(_ != null).map(x => converter.convertToJson(x)).map(x => new String(x)).toSeq
      })
    }
  } yield result
}