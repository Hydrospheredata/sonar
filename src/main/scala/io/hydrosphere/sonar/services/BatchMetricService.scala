package io.hydrosphere.sonar.services

import java.io.ByteArrayInputStream

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import com.mongodb.client.model.changestream.ChangeStreamDocument
import io.hydrosphere.serving.manager.data_profile_types.DataProfileType.NUMERICAL
import io.hydrosphere.sonar.config.Configuration
import io.hydrosphere.sonar.utils.CollectionOps
import io.hydrosphere.sonar.utils.ContractOps._
import io.hydrosphere.sonar.utils.FutureOps._
import io.hydrosphere.sonar.utils.TensorProtoOps._
import io.hydrosphere.sonar.utils.BooleanOps._
import io.hydrosphere.sonar.utils.math.{KolmogorovSmirnovTest, Statistics}
import eu.timepit.refined.auto._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.bson.json.{JsonWriterSettings, StrictJsonWriter}
import org.bson.types.ObjectId
import org.kitesdk.data.spi.filesystem.JSONFileReader
import org.kitesdk.data.spi.{JsonUtil, SchemaUtil}
import org.mongodb.scala.bson.{BsonArray, BsonObjectId}
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase}

import scala.collection.JavaConverters._
import parquet.avro.AvroParquetWriter

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
//      .map(_.map(_.toJson(
//        settings = JsonWriterSettings
//          .builder()
//          .objectIdConverter((value: ObjectId, writer: StrictJsonWriter) => writer.writeString(value.toHexString))
//          .build()
//      )))
  }

  private def onNewDoc(doc: ChangeStreamDocument[Document]): Unit = {
    val fullDoc = doc.getFullDocument
    val modelVersionId = fullDoc.getLong("_hs_model_version_id")
    val program: IO[Unit] = for {
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
//      jsons = checks.map(_.toJson(
//        settings = JsonWriterSettings
//          .builder()
//          .objectIdConverter((value: ObjectId, writer: StrictJsonWriter) => writer.writeString(value.toHexString))
//          .build()
//      ))
//      schemas = jsons.map(json => JsonUtil.inferSchema(JsonUtil.parse(json), "myschema"))
//      schema = SchemaUtil.merge(schemas)
//      _ = {
//        val reader = new JSONFileReader(new ByteArrayInputStream(s"[${jsons.mkString(",")}]".getBytes), schema, classOf[Record])
//        reader.initialize()
//        val fs = HadoopFile
//        val writer = AvroParquetWriter
//      }
      

      
    } yield ()
    program.unsafeRunAsync {
      case Left(value) => 
        println(s"EEEEEEEEEEEROR $value")
        value.printStackTrace()
      case Right(value) => println(value)
    }
    println(doc.getFullDocument.toJson())
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
