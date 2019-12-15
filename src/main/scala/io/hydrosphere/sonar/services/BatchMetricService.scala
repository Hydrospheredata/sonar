package io.hydrosphere.sonar.services

import java.io.ByteArrayInputStream

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

  lazy val database: MongoDatabase = mongoClient.getDatabase(config.mongo.database)
  lazy val checkCollection: MongoCollection[Document] =
    database.getCollection("checks")
  lazy val aggregatedCheckCollection: MongoCollection[Document] =
    database.getCollection("aggregated_check")

  private def getChecks(modelVersionId: Long, to: String): IO[Seq[Document]] = {
    checkCollection
      .find(and(lt("_id", BsonObjectId(to)), equal("_hs_model_version_id", modelVersionId)))
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
    
    var rawChecksBuilder = rootBuilder.name("_hs_raw_checks").`type`().record("_hs_raw_checks").fields()

    rawChecksBuilder.name("_hs_metrics").`type`().map().values().nullable().record("RawCheck").fields()
      .requiredBoolean("check")
      .requiredString("description")
      .requiredDouble("threshold")
      .requiredDouble("value")
      .optionalString("metricSpecId")
      .endRecord().noDefault()
    
    modelFields.foreach(modelField => {
      modelField.profile match {
        case DataProfileType.NUMERICAL =>
          rawChecksBuilder = rawChecksBuilder
            .name(modelField.name).`type`().map().values().`type`("RawCheck").noDefault()
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
      modelVersion <- modelDataService.getModelVersion(modelVersionId)
      checks <- getChecks(modelVersionId, fullDoc.getObjectId("_hs_first_id").toHexString)
      fields = modelVersion.contract.map(_.extractAllFields).getOrElse(Seq.empty)
      batchChecks: Map[String, Map[String, Map[String, Int]]] = fields.flatMap(field => field.profile match {
        case NUMERICAL => {
          // TODO: not safe Option.get
          val data = checks
            // TODO: get right numerical types
            .map(x => if (field.shape.get.isScalar) Seq[Double](x.getLong(field.name).toDouble) else x.get[BsonArray](field.name).get.asScala.map(_.asDouble().doubleValue()))
          val transposed = CollectionOps.safeTranspose(data)
          // TODO: use production distribution
          val ksFn = (sample: NonEmptyList[Double]) => KolmogorovSmirnovTest.test(sample, Statistics.generateDistribution(Statistics.Distribution.Normal, 100))
          val ksTestResults = transposed.filter(_.nonEmpty).map(l => NonEmptyList(l.head, l.toList.tail)).map(ksFn).map(result => result.value <= result.rejectionLevel).map(_.toInt)
          Seq(field.name -> Map("ks" -> Map("checked" -> ksTestResults.size, "passed" -> ksTestResults.sum)))
        }
        // TODO: implement other data types
        case _ => Seq.empty
      }).toMap
      _ <- checkStorageService.enrichAggregatesWithBatchChecks(fullDoc.getObjectId("_id").toHexString, batchChecks)
      jsons = checks.map(_.toJson(
        settings = JsonWriterSettings
          .builder()
          .objectIdConverter((value: ObjectId, writer: StrictJsonWriter) => writer.writeString(value.toHexString))
          .build()
      ))
      schema <- IO(inferSchema(fields))
      result <- IO {
        println(s"Schema: $schema")
        // TODO: remove hardcode
        val conf = new HadoopConfiguration()
        conf.set("fs.s3a.access.key", "minio")
        conf.set("fs.s3a.secret.key", "minio123")
        conf.set("fs.s3a.endpoint", "http://minio:9000")
        conf.set("fs.s3a.path.style.access", "true")
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        // TODO: create bucket
        val writer = AvroParquetWriter
          .builder[Record](new Path("s3a://feature-lake/blah.parquet"))
          .withConf(conf)
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .withSchema(schema)
          .build()
        println("Writer is built")

        val converter = new JsonAvroConverter()
        val records = jsons.map(json => converter.convertToGenericDataRecord(json.getBytes, schema))

        for (record: Record <- records) {
          println("writing record")
          writer.write(record)
        }
        println("closing writes")
        writer.close()
        "ok"
      }
    } yield result
    program.unsafeRunAsync {
      case Left(value) => 
        println(s"EEEEEEEEEEEROR $value")
        value.printStackTrace()
      case Right(value) => println(value)
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
