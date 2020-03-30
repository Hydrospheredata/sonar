package io.hydrosphere.sonar.services

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import com.mongodb.client.model.changestream.ChangeStreamDocument
import io.hydrosphere.serving.manager.data_profile_types.DataProfileType.NUMERICAL
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import io.hydrosphere.sonar.config.Configuration
import io.hydrosphere.sonar.utils.CollectionOps
import io.hydrosphere.sonar.utils.ContractOps._
import io.hydrosphere.sonar.utils.FutureOps._
import io.hydrosphere.sonar.utils.TensorProtoOps._
import io.hydrosphere.sonar.utils.BooleanOps._
import io.hydrosphere.sonar.utils.ModelFieldOps._
import io.hydrosphere.sonar.utils.math.{KolmogorovSmirnovTest, Statistics}
import eu.timepit.refined.auto._
import io.hydrosphere.serving.contract.model_field.ModelField
import io.hydrosphere.serving.manager.data_profile_types.DataProfileType
import io.hydrosphere.serving.tensorflow.types.DataType
import io.minio.MinioClient
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.GenericData.Record
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.bson.json.{JsonWriterSettings, StrictJsonWriter}
import org.bson.types.ObjectId
import org.mongodb.scala.bson.{BsonArray, BsonObjectId}
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase}
import tech.allegro.schema.json2avro.converter.JsonAvroConverter

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

trait BatchMetricService[F[_]] {
  def start: F[Unit]
}

class MongoParquetBatchMetricService[F[_]: Async](config: Configuration, mongoClient: MongoClient, modelDataService: ModelDataService[IO], checkStorageService: CheckStorageService[IO]) extends BatchMetricService[F] {
  
  case object FirstAggregationError extends Throwable

  lazy val database: MongoDatabase = mongoClient.getDatabase(config.mongo.database)
  lazy val checkCollection: MongoCollection[Document] =
    database.getCollection("checks")
  lazy val aggregatedCheckCollection: MongoCollection[Document] =
    database.getCollection("aggregated_check")

  // TODO: should be in a storage service
  private def getChecks(modelVersionId: Long, from: String, to: String): IO[Seq[Document]] = {
    checkCollection
      .find(and(and(gte("_id", BsonObjectId(from)), lte("_id", BsonObjectId(to))), equal("_hs_model_version_id", modelVersionId)))
      .toFuture()
      .liftToAsync[IO]
  }
  
  def inferSchema(modelFields: Seq[ModelField]): Schema = {
    var rootBuilder = 
      SchemaBuilder
          .record("HydroServingRequestResponse").namespace("io.hydrosphere.serving.requestresponse")
          .fields()
            .requiredString("_id")
            .requiredDouble("_hs_latency")
            .requiredLong("_hs_model_version_id")
            .requiredString("_hs_model_name")
            .requiredLong("_hs_model_incremental_version")
            .requiredString("_hs_request_id")
            .requiredLong("_hs_timestamp")
            .requiredInt("_hs_year")
            .requiredInt("_hs_month")
            .requiredInt("_hs_day")
        
            .optionalString("_hs_error")
            .optionalDouble("_hs_score")
            .optionalDouble("_hs_overall_score")
         
    modelFields.foreach(modelField => {
      modelField.eitherSubfieldOrDataType match {
        case Left(_) => // do nothing 
        case Right(dataType) =>
          def setType(scalarFn: SchemaBuilder.BaseFieldTypeBuilder[Schema] => SchemaBuilder.FieldAssembler[Schema], arrayFn: SchemaBuilder.TypeBuilder[SchemaBuilder.ArrayDefault[Schema]] => SchemaBuilder.ArrayDefault[Schema]): SchemaBuilder.FieldAssembler[Schema] = {
            if (modelField.shape.get.isScalar) {
              scalarFn(rootBuilder.name(modelField.name).`type`().nullable())
            } else {
              arrayFn(rootBuilder.name(modelField.name).`type`().array().items()).noDefault()
            }
          }

          rootBuilder = dataType match {
            case DataType.DT_INVALID => rootBuilder // do nothing
            case DataType.DT_FLOAT => setType(_.floatType().noDefault(), _.floatType())
            case DataType.DT_DOUBLE => setType(_.doubleType().noDefault(), _.doubleType())
            case DataType.DT_INT32 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_UINT8 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_INT16 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_INT8 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_STRING => setType(_.stringType().noDefault(), _.stringType())
            case DataType.DT_COMPLEX64 => setType(_.doubleType().noDefault(), _.doubleType())
            case DataType.DT_INT64 => setType(_.longType().noDefault(), _.longType())
            case DataType.DT_BOOL => setType(_.booleanType().noDefault(), _.booleanType())
            case DataType.DT_QINT8 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_QUINT8 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_QINT32 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_BFLOAT16 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_QINT16 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_QUINT16 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_UINT16 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_COMPLEX128 => setType(_.doubleType().noDefault(), _.doubleType())
            case DataType.DT_HALF => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_RESOURCE => rootBuilder // do nothing
            case DataType.DT_VARIANT => rootBuilder // do nothing
            case DataType.DT_UINT32 => setType(_.longType().noDefault(), _.longType())
            case DataType.DT_UINT64 => setType(_.longType().noDefault(), _.longType())
            case DataType.DT_MAP => rootBuilder // do nothing
            case DataType.Unrecognized(value) => rootBuilder // do nothing
          }
          rootBuilder = rootBuilder.name(s"_hs_${modelField.name}_score").`type`().nullable().doubleType().noDefault()
      }
    })

    rootBuilder = rootBuilder.name("_hs_metric_checks").`type`().map().values().nullable().record("RawCheck").fields()
      .requiredBoolean("check")
      .requiredString("description")
      .requiredDouble("threshold")
      .requiredDouble("value")
      .optionalString("metricSpecId")
      .endRecord().noDefault()

    var rawChecksBuilder = rootBuilder.name("_hs_raw_checks").`type`().record("_hs_raw_checks").fields()
    
    modelFields.foreach(modelField => {
      modelField.profile match {
        case DataProfileType.NUMERICAL =>
          rawChecksBuilder = rawChecksBuilder
            .name(modelField.name).`type`().array().items().`type`("RawCheck").noDefault()
        case DataProfileType.NONE => // do nothing
        case DataProfileType.CATEGORICAL => // do nothing
        case DataProfileType.NOMINAL => // do nothing
        case DataProfileType.ORDINAL => // do nothing
        case DataProfileType.CONTINUOUS => // do nothing
        case DataProfileType.INTERVAL => // do nothing
        case DataProfileType.RATIO => // do nothing
        case DataProfileType.IMAGE => // do nothing
        case DataProfileType.VIDEO => // do nothing
        case DataProfileType.AUDIO => // do nothing
        case DataProfileType.TEXT => // do nothing
        case DataProfileType.Unrecognized(value) => // do nothing
      }
    })
    
    rootBuilder = rawChecksBuilder.endRecord().noDefault()
    
    rootBuilder.endRecord()
  }

  private def onNewDoc(doc: ChangeStreamDocument[Document]): Unit = {
    val fullDoc = doc.getFullDocument
    val modelVersionId = fullDoc.getLong("_hs_model_version_id")
    val program: IO[String] = for {
      maybeAggregation <- checkStorageService.getPreviousAggregate(modelVersionId, fullDoc.getObjectId("_id").toHexString)
      aggregation <- maybeAggregation match {
        case Some(value) => value.pure[IO]
        case None => IO.raiseError(FirstAggregationError)
      }
      modelVersion <- modelDataService.getModelVersion(modelVersionId)
      checks <- getChecks(modelVersionId, aggregation.getObjectId("_hs_first_id").toHexString, aggregation.getObjectId("_hs_last_id").toHexString)
      fields = modelVersion.contract.map(_.extractAllFields).getOrElse(Seq.empty)
      batchChecks: Map[String, Map[String, Map[String, Int]]] = fields.flatMap(field => field.profile match {
        case NUMERICAL => {
          // TODO: not safe Option.get
          val data = checks
            // TODO: get right numerical types
            .map(x => if (field.shape.get.isScalar) Seq[Double](x.getLong(field.name).toDouble) else x.get[BsonArray](field.name).get.asScala.map(_.asDouble().doubleValue()))
          val transposed = CollectionOps.safeTranspose(data)
          // TODO: use training distribution
          val ksFn = (sample: NonEmptyList[Double]) => KolmogorovSmirnovTest.test(sample, Statistics.generateDistribution(Statistics.Distribution.Normal, 100))
          val ksTestResults = transposed.filter(_.nonEmpty).map(l => NonEmptyList(l.head, l.toList.tail)).map(ksFn).map(result => result.value <= result.rejectionLevel).map(_.toInt)
          Seq(field.name -> Map("ks" -> Map("checked" -> ksTestResults.size, "passed" -> ksTestResults.sum)))
        }
        // TODO: implement other data types
        case _ => Seq.empty
      }).toMap
      _ <- checkStorageService.enrichAggregatesWithBatchChecks(fullDoc.getObjectId("_id").toHexString, batchChecks)
      groupedByPath = checks.groupBy(check => {
        s"${check.getString("_hs_model_name")}/${check.getLong("_hs_model_incremental_version")}/_hs_year=${check.getInteger("_hs_year")}/_hs_month=${check.getInteger("_hs_month")}/_hs_day=${check.getInteger("_hs_day")}/${aggregation.getObjectId("_id").toHexString}.parquet"
      })

      schema = inferSchema(fields)
//      _ = println(schema.toString(true))
      _ <- IO[Unit] {
        val minio = S3Client.fromConfig(config)
        val exists = minio.bucketExists(config.storage.bucket)
        if (!exists && config.storage.createBucket) {
          minio.makeBucket(config.storage.bucket)
        }
      }
      result <- IO {
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
        
        val jsonWriterSettings = JsonWriterSettings
          .builder()
          .objectIdConverter((value: ObjectId, writer: StrictJsonWriter) => writer.writeString(value.toHexString))
          .build()
        
        groupedByPath.foreach({case (path, docs) =>
          val writer = AvroParquetWriter
            .builder[Record](new Path(s"s3a://${config.storage.bucket}/${path}"))
            .withConf(conf)
            .withCompressionCodec(CompressionCodecName.GZIP)
            .withSchema(schema)
            .build()

          val converter = new JsonAvroConverter()
          val records = docs
            .map(_.toJson(jsonWriterSettings))
            .map(json => converter.convertToGenericDataRecord(json.getBytes, schema))

          for (record: Record <- records) {
            writer.write(record)
          }
          writer.close()
          println(s"Written ${records.size} rows to ${path}")
        })
        
        "ok"
      }
    } yield result
    program.unsafeRunAsync {
      case Left(value) => value match {
        case FirstAggregationError => // do nothing
        case _ => 
          println(s"EEEEEEEEEEEROR $value")
          value.printStackTrace()
      }
      case Right(value) => // do nothing
    }
  }
  
  override def start: F[Unit] = {
    Sync[F].delay {
      aggregatedCheckCollection.watch(Seq(`match`(equal("operationType", "insert")))).subscribe(
        onNewDoc _,
        (t: Throwable) => println(s"ERROROROROROR $t"),
        () => println("DONENENENENENEN")
      )
      Unit
    }
  }
  
}
