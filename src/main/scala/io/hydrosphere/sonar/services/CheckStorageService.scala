package io.hydrosphere.sonar.services

import cats.effect.{Async, Sync}
import cats.effect.concurrent.Ref
import cats.implicits._
import io.hydrosphere.serving.manager.grpc.entities.ModelVersion
import io.hydrosphere.serving.monitoring.api.ExecutionInformation
import io.hydrosphere.serving.tensorflow.types.DataType
import io.hydrosphere.sonar.Logging
import io.hydrosphere.sonar.config.Configuration
import io.hydrosphere.sonar.terms.Check
import io.hydrosphere.sonar.utils.ModelFieldOps._
import io.hydrosphere.sonar.utils.ExecutionInformationOps._
import io.hydrosphere.sonar.utils.BooleanOps._
import io.hydrosphere.sonar.utils.FutureOps._
import org.bson.codecs.ObjectIdGenerator
import org.bson.json.{Converter, JsonWriterSettings, StrictJsonWriter}
import org.bson.types.ObjectId
import org.mongodb.scala.bson.{BsonArray, BsonBoolean, BsonDocument, BsonNull, BsonNumber, BsonObjectId, BsonString, BsonValue}
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase}
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.UpdateOptions

object CheckStorageService {
  type CheckedRequest = (ExecutionInformation, Map[String, Seq[Check]])
}

trait CheckStorageService[F[_]] {
  def saveCheckedRequest(request: ExecutionInformation, modelVersion: ModelVersion, checks: Map[String, Seq[Check]]): F[Unit]

  // TODO: can we make this better than returning json representation as a String?
  def getChecks(modelVersionId: Long, from: String, to: String): F[Seq[String]]
  def getAggregates(modelVersionId: Long, limit: Int, offset: Int): F[Seq[String]]
}

class InMemoryCheckStorageService[F[_]: Sync](state: Ref[F, Seq[CheckStorageService.CheckedRequest]]) extends CheckStorageService[F] {
  override def saveCheckedRequest(request: ExecutionInformation, modelVersion: ModelVersion, checks: Map[String, Seq[Check]]): F[Unit] = for {
    stored <- state.get
    _ <- state.set(stored :+ (request, checks))
  } yield Unit

  override def getChecks(modelVersionId: Long, from: String, to: String): F[Seq[String]] = Sync[F].pure(Seq.empty)
  override def getAggregates(modelVersionId: Long, limit: Int, offset: Int): F[Seq[String]] = Sync[F].pure(Seq.empty)
}

class MongoCheckStorageService[F[_]: Async](config: Configuration, mongoClient: MongoClient) extends CheckStorageService[F] with Logging {
  
  lazy val database: MongoDatabase = mongoClient.getDatabase(config.mongo.database)

  lazy val checkCollection: MongoCollection[Document] =
    database.getCollection("checks")
  lazy val aggregatedCheckCollection: MongoCollection[Document] =
    database.getCollection("aggregated_check")
  
  private def getScore(checks: Iterable[Check]): BsonValue = {
    val checkValues = checks.map(_.check.toInt)
    val score = checkValues.sum.toDouble / checkValues.size
    if (score.isNaN) BsonNull() else BsonNumber(score)
    
  }


  override def getChecks(modelVersionId: Long, from: String, to: String): F[Seq[String]] = {
    checkCollection
      .find(and(and(gte("_id", BsonObjectId(from)), lte("_id", BsonObjectId(to))), equal("_hs_model_version_id", modelVersionId)))
      .toFuture()
      .liftToAsync[F]
      .map(_.map(_.toJson(
        settings = JsonWriterSettings
          .builder()
          .objectIdConverter((value: ObjectId, writer: StrictJsonWriter) => writer.writeString(value.toHexString))
          .build()
      )))
  }
  
  override def getAggregates(modelVersionId: Long, limit: Int, offset: Int): F[Seq[String]] = {
    aggregatedCheckCollection
      .find(equal("_hs_model_version_id", modelVersionId))
      .limit(limit)
      .skip(offset)
      .toFuture()
      .liftToAsync[F]
      .map(_.map(_.toJson(
        settings = JsonWriterSettings
          .builder()
          .objectIdConverter((value: ObjectId, writer: StrictJsonWriter) => writer.writeString(value.toHexString))
          .build()
      )))
  }

  override def saveCheckedRequest(ei: ExecutionInformation, modelVersion: ModelVersion, checks: Map[String, Seq[Check]]): F[Unit] = {
    logger.info(s"saveCheckedRequest with ${checks.size} checks")
    case class ByFieldChecks(checks: Seq[(String, BsonValue)], aggregates: Seq[(String, BsonValue)])
    
    val maybeBsonChecks = for {
      contract <- modelVersion.contract
      predict <- contract.predict
      fields = predict.inputs ++ predict.outputs
      request <- ei.request
      response <- ei.eitherResponseOrError.toOption // TODO: there will be no metrics if error
      data = request.inputs ++ response.outputs
    } yield fields.map { modelField =>
      val bsonValue = modelField.eitherSubfieldOrDataType match {
        case Left(_) => BsonNull() // TODO: process subfields
        case Right(dataType) => dataType match { // TODO: refactor
          case DataType.DT_FLOAT =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.floatVal.map(value => BsonNumber(value)))).getOrElse(BsonNull())
          case DataType.DT_DOUBLE =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.doubleVal.map(value => BsonNumber(value)))).getOrElse(BsonNull())
          case DataType.DT_INT32 =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.intVal.map(value => BsonNumber(value)))).getOrElse(BsonNull())
          case DataType.DT_UINT8 =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.intVal.map(value => BsonNumber(value)))).getOrElse(BsonNull())
          case DataType.DT_INT16 =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.intVal.map(value => BsonNumber(value)))).getOrElse(BsonNull())
          case DataType.DT_INT8 =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.intVal.map(value => BsonNumber(value)))).getOrElse(BsonNull())
          case DataType.DT_STRING =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.stringVal.map(value => BsonString(value.toStringUtf8)))).getOrElse(BsonNull())
          case DataType.DT_COMPLEX64 =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.scomplexVal.map(value => BsonNumber(value)))).getOrElse(BsonNull())
          case DataType.DT_INT64 =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.int64Val.map(value => BsonNumber(value)))).getOrElse(BsonNull())
          case DataType.DT_BOOL =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.boolVal.map(value => BsonBoolean(value)))).getOrElse(BsonNull())
          case DataType.DT_QINT8 =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.intVal.map(value => BsonNumber(value)))).getOrElse(BsonNull())
          case DataType.DT_QUINT8 =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.intVal.map(value => BsonNumber(value)))).getOrElse(BsonNull())
          case DataType.DT_QINT32 =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.intVal.map(value => BsonNumber(value)))).getOrElse(BsonNull())
          case DataType.DT_BFLOAT16 =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.halfVal.map(value => BsonNumber(value)))).getOrElse(BsonNull())
          case DataType.DT_QINT16 =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.intVal.map(value => BsonNumber(value)))).getOrElse(BsonNull())
          case DataType.DT_QUINT16 =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.intVal.map(value => BsonNumber(value)))).getOrElse(BsonNull())
          case DataType.DT_UINT16 =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.intVal.map(value => BsonNumber(value)))).getOrElse(BsonNull())
          case DataType.DT_COMPLEX128 =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.dcomplexVal.map(value => BsonNumber(value)))).getOrElse(BsonNull())
          case DataType.DT_HALF =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.halfVal.map(value => BsonNumber(value)))).getOrElse(BsonNull())
          case DataType.DT_UINT32 =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.uint32Val.map(value => BsonNumber(value)))).getOrElse(BsonNull())
          case DataType.DT_UINT64 =>
            data.get(modelField.name).map(tensorProto => BsonArray(tensorProto.uint64Val.map(value => BsonNumber(value)))).getOrElse(BsonNull())
          case DataType.DT_MAP =>
            logger.warn(s"Cannot serialize DT_MAP type for `${modelField.name}` field")
            BsonNull() // TODO: serialize DT_MAP type
          case DataType.DT_RESOURCE =>
            logger.warn(s"Cannot serialize DT_RESOURCE type for `${modelField.name}` field")
            BsonNull() // TODO: serialize DT_RESOURCE type
          case DataType.DT_VARIANT =>
            logger.warn(s"Cannot serialize DT_VARIANT type for `${modelField.name}` field")
            BsonNull() // TODO: serialize DT_VARIANT type
          case DataType.DT_INVALID =>
            logger.warn(s"DataType is DT_INVALID for `${modelField.name}`")
            BsonNull()
          case DataType.Unrecognized(value) => BsonNull()
        }
      }
      val fieldChecks = checks.getOrElse(modelField.name, Seq.empty)
      val bsonScore = getScore(fieldChecks)
      val aggregates = Seq(
        s"${modelField.name}.checks" -> BsonNumber(fieldChecks.size), 
        s"${modelField.name}.passed" -> BsonNumber(fieldChecks.map(_.check.toInt).sum)
      )
      logger.info(s"""${modelField.name} -> $bsonValue, _hs_${modelField.name}_score -> $bsonScore""")
      val checkValues = Seq(
        modelField.name -> bsonValue, 
        s"_hs_${modelField.name}_score" -> bsonScore
      )
      ByFieldChecks(checkValues, aggregates)
    }.foldLeft(ByFieldChecks(Seq.empty, Seq.empty))((a, b) => ByFieldChecks(a.checks ++ b.checks, a.aggregates ++ b.aggregates))
    val bsonLatency = ei.metadata.map(m => BsonNumber(m.latency)).getOrElse(BsonNull())
    logger.info(s"""latency $bsonLatency""")
    val bsonError = ei.eitherResponseOrError.left.toOption.map(e => BsonString(e.errorMessage)).getOrElse(BsonNull())
    logger.info(s"""error $bsonError""")
    val bsonScore = getScore(checks.values.flatten)
    logger.info(s"""score $bsonScore""")
    val overallScore = getScore(checks.getOrElse("overall", Seq.empty))
    logger.info(s"""overall $overallScore""")
    val objectId = BsonObjectId()
    
    val rawChecks = checks.map {
      case (field, checkList) => 
        field -> BsonArray(checkList.flatMap(check => {
          Seq(
            "check" -> BsonBoolean(check.check),
            "description" -> BsonString(check.description),
            "threshold" -> BsonNumber(check.threshold),
            "value" -> BsonNumber(check.value),
            "metricSpecId" -> check.metricSpecId.map(BsonString.apply).getOrElse(BsonNull())
          )
        }))
//        s"_hs_raw_checks.$field.check" -> BsonArray(checkList.map(_.check).map(BsonBoolean.apply)),
//        s"_hs_raw_checks.$field.description" -> BsonArray(checkList.map(_.description).map(BsonString.apply)),
//        s"_hs_raw_checks.$field.threshold" -> BsonArray(checkList.map(_.threshold).map(BsonNumber.apply)),
//        s"_hs_raw_checks.$field.value" -> BsonArray(checkList.map(_.value).map(BsonNumber.apply))
    }
    
    val bsonChecks = maybeBsonChecks.map(_.checks).getOrElse(Seq.empty) :+ 
      ("_hs_raw_checks" -> BsonDocument(rawChecks.toSeq)) :+
      ("_hs_latency" -> bsonLatency) :+
      ("_hs_error" -> bsonError) :+
      ("_hs_score" -> bsonScore) :+
      ("_hs_overall_score" -> overallScore) :+
      ("_hs_model_version_id" -> BsonNumber(modelVersion.id)) :+
      ("_id" -> objectId)
    val insertDocument = Document(bsonChecks)
    
    val increments = maybeBsonChecks.map(_.aggregates).getOrElse(Seq.empty) :+ ("_hs_requests" -> BsonNumber(1))
    val aggregateQuery = Document(
      "$inc" -> increments,
      "$set" -> Document("_hs_last_id" -> objectId),
      "$setOnInsert" -> Document("_hs_first_id" -> objectId, "_hs_model_version_id" -> BsonNumber(modelVersion.id))
    )

    checkCollection.insertOne(insertDocument).toFuture().liftToAsync *> 
      aggregatedCheckCollection.updateOne(
        filter = and(equal("_hs_model_version_id", modelVersion.id), lt("_hs_requests", 10)), // TODO: configure
        update = aggregateQuery,
        options = (new UpdateOptions).upsert(true)
      ).toFuture().liftToAsync.map(_ => Unit)
    
//    for {
//      _ <- 
//      _ <
//    } yield Unit
  }
}
