package io.hydrosphere.sonar.services

import cats.effect.concurrent.Ref
import cats.effect.Async
import cats.implicits._
import com.google.protobuf.ByteString
import io.hydrosphere.serving.proto.manager.entities.ModelVersion
import io.hydrosphere.serving.proto.contract.types.DataType
import io.hydrosphere.serving.proto.contract.tensor.Tensor
import io.hydrosphere.monitoring.proto.sonar.entities.ExecutionInformation
import io.hydrosphere.sonar.Logging
import io.hydrosphere.sonar.services.ModelDataService
import io.hydrosphere.sonar.config.Configuration
import io.hydrosphere.sonar.terms.{AggregationMetadata, Check}
import io.hydrosphere.sonar.utils.BooleanOps._
import io.hydrosphere.sonar.utils.ExecutionInformationOps._
import io.hydrosphere.sonar.utils.FutureOps._
import io.hydrosphere.sonar.utils.ModelFieldOps._
import io.hydrosphere.sonar.utils.TensorProtoOps._
import io.hydrosphere.sonar.utils.mongo.ObservableExt._
import org.bson.json.{JsonWriterSettings, StrictJsonWriter}
import org.bson.types.ObjectId
import org.mongodb.scala.bson.{BsonArray, BsonBoolean, BsonDateTime, BsonDocument, BsonNull, BsonNumber, BsonObjectId, BsonString, BsonValue, conversions}
import org.mongodb.scala.model.Filters.{lt, _}
import org.mongodb.scala.model.{Accumulators, FindOneAndUpdateOptions, UpdateOptions}
import org.mongodb.scala.model.Sorts.{descending, _}
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.{ClientSession, Completed, Document, MongoClient, MongoCollection, MongoDatabase, Observable, Observer, ReadPreference, SingleObservable, TransactionOptions}
import java.util.Calendar
import java.util.GregorianCalendar

import com.mongodb.client.model.Indexes
import com.mongodb.{MongoException, ReadConcern, WriteConcern}
import org.mongodb.scala.bson.collection.mutable
import org.mongodb.scala.result.UpdateResult

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.collection.JavaConverters._
import scala.util.Random


object CheckStorageService {
  type CheckedRequest = (ExecutionInformation, Map[String, Seq[Check]])
}

// TODO: split the trait into checks/aggregates for slow/fast storages
trait CheckStorageService[F[_]] {
  def saveCheckedRequest(request: ExecutionInformation, modelVersion: ModelVersion, checks: Map[String, Seq[Check]], metricChecks: Map[String, Check]): F[Unit]

  def getAggregateCount(modelVersionId: Long, from: Option[Int], till: Option[Int]): F[Long]

  def getAggregateDateRange(modelVersionId: Long): F[Option[(Int, Int)]]

  def getAggregateById(modelVersionId: Long, aggregationId: String): F[Option[Document]]

  def getChecksForComparision(originalModelVersion: Long, aggregationId: String, comparedModelVersionId: Long): F[Seq[String]]

  // TODO:  Map[String, Map[String, Map[String, Int]]] should be case class (maybe `Check`?)
  def enrichAggregatesWithBatchChecks(aggregationId: String, checks: Map[String, Map[String, Map[String, Int]]]): F[Unit]

  // TODO: can we make this better than returning json representation as a String?
  def getChecks(modelVersionId: Long, from: String, to: String): F[Seq[String]]
  def getChecks(modelVersionId: Long, limit: Int, offset: Int): F[Seq[String]]
  def getChecksByAggregationId(modelVersionId: Long, aggregationId: String): F[Seq[String]]
  def getAggregates(modelVersionId: Long, limit: Int, offset: Int, from: Option[Int], till: Option[Int]): F[Seq[String]]
  def getCheckById(id: String): F[Option[String]]
  def getCheckSubsample(modelVersionId: Long, size: Int): F[Seq[String]]

  // TODO: it should not be mongo specific type
  def getPreviousAggregate(modelVersionId: Long, nextAggregationId: String): F[Option[Document]]
}

class MongoCheckStorageService[F[_]: Async](config: Configuration, mongoClient: MongoClient, modelDataService: ModelDataService[F]) extends CheckStorageService[F] with Logging {

  lazy val database: MongoDatabase = mongoClient.getDatabase(config.mongo.database)

  lazy val checkCollection: MongoCollection[Document] =
    database.getCollection("checks")
  lazy val aggregatedCheckCollection: MongoCollection[Document] =
    database.getCollection("aggregated_check")

  def createIndexesIfNotExist(): F[Unit] = Async[F].delay {
    checkCollection.createIndex(Indexes.descending("_hs_model_version_id")).toFuture().liftToAsync[F] *>
      checkCollection.createIndex(Indexes.hashed("_hs_request_id")).toFuture().liftToAsync[F] *>
      aggregatedCheckCollection.createIndex(Indexes.descending("_hs_model_version_id")).toFuture().liftToAsync[F] *>
      aggregatedCheckCollection.createIndex(Indexes.descending("_hs_first_id")).toFuture().liftToAsync[F] *>
      aggregatedCheckCollection.createIndex(Indexes.descending("_hs_last_id")).toFuture().liftToAsync[F].map(_ => Unit)
  }

  
  private def getScore(checks: Iterable[Check]): BsonValue = {
    val checkValues = checks.map(_.check.toInt)
    val score = checkValues.sum.toDouble / checkValues.size
    if (score.isNaN) BsonNull() else BsonNumber(score)
  }

  override def getChecksForComparision(originalModelVersion: Long, aggregationId: String, comparedModelVersionId: Long): F[Seq[String]] = for {
    maybeAggregate <- getAggregateById(originalModelVersion, aggregationId)
    aggregate = maybeAggregate.get
    requestIds <- checkCollection.find(and(equal("_hs_model_version_id", originalModelVersion), and(gte("_id", BsonObjectId(aggregate.getObjectId("_hs_first_id").toHexString)), lte("_id", BsonObjectId(aggregate.getObjectId("_hs_last_id").toHexString))))).toF[F].map(_.map(doc => doc.getString("_hs_request_id")))
    _ <- Async[F].delay(println(requestIds))
    checks <- checkCollection.find(and(equal("_hs_model_version_id", comparedModelVersionId), in("_hs_request_id", requestIds: _*))).toF[F].map(_.map(_.toJson(
      settings = JsonWriterSettings
        .builder()
        .objectIdConverter((value: ObjectId, writer: StrictJsonWriter) => writer.writeString(value.toHexString))
        .build()
    )))
  } yield checks


  override def getChecksByAggregationId(modelVersionId: Long, aggregationId: String): F[Seq[String]] = for {
    maybeAggregate <- getAggregateById(modelVersionId, aggregationId)
    ids = maybeAggregate.flatMap {
      _.get[BsonArray]("_hs_request_ids").map(arr => arr.getValues.asScala.map(_.asObjectId()))
    }.getOrElse(List.empty)
    result <- checkCollection
      .find(and(in("_id", ids:_*), equal("_hs_model_version_id", modelVersionId)))
      .toFuture()
      .liftToAsync[F]
      .map(_.map(_.toJson(
        settings = JsonWriterSettings
          .builder()
          .objectIdConverter((value: ObjectId, writer: StrictJsonWriter) => writer.writeString(value.toHexString))
          .build()
      )))
  } yield result


  override def getAggregateById(modelVersionId: Long, aggregationId: String): F[Option[Document]] =
    aggregatedCheckCollection
      .find(and(equal("_hs_model_version_id", modelVersionId), equal("_id", new ObjectId(aggregationId))))
      .limit(1)
      .toFuture()
      .liftToAsync[F]
      .map(_.headOption)


  override def getPreviousAggregate(modelVersionId: Long, nextAggregationId: String): F[Option[Document]] = {
    aggregatedCheckCollection
      .find(and(lt("_id", new ObjectId(nextAggregationId)), equal("_hs_model_version_id", modelVersionId)))
      .sort(descending("_id"))
      .limit(1)
      .toFuture()
      .liftToAsync[F]
      .map(_.headOption)
  }

  override def getAggregateCount(modelVersionId: Long, from: Option[Int], till: Option[Int]): F[Long] = {
    val idFilter = equal("_hs_model_version_id", modelVersionId)
    val fromFilter = from.map(ts => gte("_hs_first_id", new ObjectId(new java.util.Date(ts.toLong * 1000))))
    val tillFilter = till.map(ts => lte("_hs_last_id", new ObjectId(new java.util.Date(ts.toLong * 1000))))
    val filters = Seq(Some(idFilter), fromFilter, tillFilter).flatten
    val productFilter = and(filters: _*)
    aggregatedCheckCollection
      .countDocuments(productFilter)
      .toFuture()
      .liftToAsync[F]
  }

  override def getAggregateDateRange(modelVersionId: Long): F[Option[(Int, Int)]] = {
    aggregatedCheckCollection
      .aggregate(List(filter(equal("_hs_model_version_id", modelVersionId)), group("null", Accumulators.min("min_date", "$_hs_first_id"), Accumulators.max("max_date", "$_hs_last_id"))))
      .toFuture()
      .liftToAsync[F]
      .map(docs => {
        for {
          doc <- docs.headOption
          minDate <- Option(doc.getObjectId("min_date")).map(_.getTimestamp)
          maxDate <- Option(doc.getObjectId("max_date")).map(_.getTimestamp)
        } yield (minDate, maxDate)
      })
  }

  override def enrichAggregatesWithBatchChecks(aggregationId: String, checks: Map[String, Map[String, Map[String, Int]]]): F[Unit] = {
    aggregatedCheckCollection
      .updateOne(equal("_id", new ObjectId(aggregationId)), set("_hs_batch", checks))
      .toFuture()
      .liftToAsync[F]
      .map(_ => Unit)
  }

  override def getChecks(modelVersionId: Long, from: String, to: String): F[Seq[String]] = {
    aggregatedCheckCollection
      .find(and(equal("_hs_first_id", new ObjectId(from)), equal("_hs_last_id", new ObjectId(to))))
      .toFuture()
      .liftToAsync[F]
      .flatMap((docs: Seq[Document]) => {
        val ids = docs.flatMap(doc => doc.get[BsonArray]("_hs_request_ids").map(arr => arr.getValues.asScala.map(_.asObjectId())).getOrElse(List.empty))
        checkCollection
          .find(and(in("_id", ids:_*), equal("_hs_model_version_id", modelVersionId)))
          .toFuture()
          .liftToAsync[F]
          .map(_.map(_.toJson(
            settings = JsonWriterSettings
              .builder()
              .objectIdConverter((value: ObjectId, writer: StrictJsonWriter) => writer.writeString(value.toHexString))
              .build()
          ))) 
      })
  }


  override def getChecks(modelVersionId: Long, limit: Int, offset: Int): F[Seq[String]] = {
    checkCollection
      .find(equal("_hs_model_version_id", modelVersionId))
      .sort(descending("_id"))
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


  override def getCheckById(id: String): F[Option[String]] = {
    checkCollection
      .find(equal("_id", BsonObjectId(id)))
      .limit(1)
      .toFuture()
      .liftToAsync[F]
      .map(_.headOption.map(_.toJson(
        settings = JsonWriterSettings
          .builder()
          .objectIdConverter((value: ObjectId, writer: StrictJsonWriter) => writer.writeString(value.toHexString))
          .build()
        )
      ))
  }


  override def getCheckSubsample(modelVersionId: Long, size: Int): F[Seq[String]] =
    checkCollection
      .aggregate(List(
        `match`(equal("_hs_model_version_id", modelVersionId)),
        Document("$sample" -> Document("size" -> size))
      ))
      .toFuture()
      .liftToAsync[F]
      .map(_.map(_.toJson(
        settings = JsonWriterSettings
          .builder()
          .objectIdConverter((value: ObjectId, writer: StrictJsonWriter) => writer.writeString(value.toHexString))
          .build()
      )))

  override def getAggregates(modelVersionId: Long, limit: Int, offset: Int, from: Option[Int], till: Option[Int]): F[Seq[String]] = {
    val idFilter = equal("_hs_model_version_id", modelVersionId)
    val fromFilter = from.map(ts => gte("_hs_last_id", new ObjectId(new java.util.Date(ts.toLong * 1000))))
    val tillFilter = till.map(ts => lte("_hs_first_id", new ObjectId(new java.util.Date(ts.toLong * 1000))))
    val filters = Seq(Some(idFilter), fromFilter, tillFilter).flatten
    val productFilter = and(filters: _*)
    aggregatedCheckCollection
//      .find(equal("_hs_model_version_id", modelVersionId))
      .find(productFilter)
      .sort(descending("_id"))
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

  // TODO: refactor (may be create more typed data structures for enrcihed data and for aggregations)
  override def saveCheckedRequest(ei: ExecutionInformation, modelVersion: ModelVersion, checks: Map[String, Seq[Check]], metricChecks: Map[String, Check]): F[Unit] = {
//    logger.info(s"${modelVersion.id} saveCheckedRequest with ${checks.size} checks and ${metricChecks.size} metrics")
    case class ByFieldChecks(checks: Seq[(String, BsonValue)], aggregates: Seq[(String, BsonValue)])

    def tensorDataToBson(tensorProto: Tensor): BsonValue = {
      val isScalar = tensorProto.tensorShape.exists(_.isScalar)
      def getter[A](data: Seq[A], bsonValue: A => BsonValue): BsonValue = {
        if (isScalar) {
          data.headOption.map(bsonValue).getOrElse(BsonNull())
        } else {
          BsonArray(data.map(bsonValue))
        }
      }
      tensorProto.dtype match {
        case DataType.DT_FLOAT => getter[Float](tensorProto.floatVal, v => BsonNumber(v.toDouble))
        case DataType.DT_DOUBLE => getter[Double](tensorProto.doubleVal, BsonNumber.apply)
        case DataType.DT_INT32 => getter(tensorProto.intVal, BsonNumber.apply)
        case DataType.DT_UINT8 => getter(tensorProto.intVal, BsonNumber.apply)
        case DataType.DT_INT16 => getter(tensorProto.intVal, BsonNumber.apply)
        case DataType.DT_INT8 => getter(tensorProto.intVal, BsonNumber.apply)
        case DataType.DT_STRING => getter[ByteString](tensorProto.stringVal, v => BsonString(v.toStringUtf8))
        case DataType.DT_COMPLEX64 =>   getter[Float](tensorProto.scomplexVal, v => BsonNumber(v.toDouble))
        case DataType.DT_INT64 => getter[Long](tensorProto.int64Val, BsonNumber.apply)
        case DataType.DT_BOOL => getter(tensorProto.boolVal, BsonBoolean.apply)
        case DataType.DT_QINT8 => getter(tensorProto.intVal, BsonNumber.apply)
        case DataType.DT_QUINT8 => getter(tensorProto.intVal, BsonNumber.apply)
        case DataType.DT_QINT32 => getter(tensorProto.intVal, BsonNumber.apply)
        case DataType.DT_QINT16 => getter(tensorProto.intVal, BsonNumber.apply)
        case DataType.DT_QUINT16 => getter(tensorProto.intVal, BsonNumber.apply)
        case DataType.DT_UINT16 => getter(tensorProto.intVal, BsonNumber.apply)
        case DataType.DT_COMPLEX128 => getter[Double](tensorProto.dcomplexVal, BsonNumber.apply)
        case DataType.DT_HALF => getter(tensorProto.halfVal, BsonNumber.apply)
        case DataType.DT_UINT32 => getter(tensorProto.uint32Val, BsonNumber.apply)
        case DataType.DT_UINT64 => getter[Long](tensorProto.uint64Val, BsonNumber.apply)
        case DataType.DT_MAP =>
          logger.warn(s"Cannot serialize DT_MAP")
          BsonNull() // TODO: serialize DT_MAP type
        case DataType.DT_INVALID =>
          logger.warn(s"DataType is DT_INVALID")
          BsonNull()
        case DataType.Unrecognized(value) => BsonNull()
      }
    }

    val maybeFields = for {
      signature <- modelVersion.signature
    } yield signature.inputs ++ signature.outputs

    val maybeBsonChecks = for {
      fields <- maybeFields
      request <- ei.request
      response <- ei.eitherResponseOrError.toOption // TODO: there will be no metrics if error
      data = request.inputs ++ response.outputs
    } yield fields.map { modelField =>
      val bsonValue = modelField.eitherSubfieldOrDataType match {
        case Left(_) => BsonNull() // TODO: process subfields
        case Right(_) => data.get(modelField.name).map(tensorDataToBson).getOrElse(BsonNull())
      }
      val fieldChecks = checks.getOrElse(modelField.name, Seq.empty)
      val bsonScore = getScore(fieldChecks)
      // TODO: find more elegant way to get right values for numerical profiles
      val aggregates = if (modelField.profile.isNumerical) {
        val reducedChecks = fieldChecks.map(_.check).grouped(2).map(_.reduce(_ && _).toInt).toSeq
        Seq(
          s"${modelField.name}.checks" -> BsonNumber(reducedChecks.size),
          s"${modelField.name}.passed" -> BsonNumber(reducedChecks.sum)
        )
      } else {
        Seq(
          s"${modelField.name}.checks" -> BsonNumber(fieldChecks.size),
          s"${modelField.name}.passed" -> BsonNumber(fieldChecks.map(_.check.toInt).sum)
        )
      }
      val checkValues = Seq(
        modelField.name -> bsonValue,
        s"_hs_${modelField.name}_score" -> bsonScore
      )
      ByFieldChecks(checkValues, aggregates)
    }.foldLeft(ByFieldChecks(Seq.empty, Seq.empty))((a, b) => ByFieldChecks(a.checks ++ b.checks, a.aggregates ++ b.aggregates))


    val bsonLatency = ei.metadata.map(m => BsonNumber(m.latency)).getOrElse(BsonNull())
    val bsonError = ei.eitherResponseOrError.left.toOption.map(BsonString(_)).getOrElse(BsonNull())
    val bsonScore = getScore(checks.values.flatten)
    val overallScore = getScore(metricChecks.values)
    val objectId = BsonObjectId()

    val rawChecks = checks.map {
      case (field, checkMap) =>
        field -> BsonArray(checkMap.map(check => {
          BsonDocument(
            "check" -> BsonBoolean(check.check),
            "description" -> BsonString(check.description),
            "threshold" -> BsonNumber(check.threshold),
            "value" -> BsonNumber(check.value),
            "metricSpecId" -> BsonNull()
          )
        }))
    }
    val allRawChecks = maybeFields.getOrElse(Seq.empty).map(field => {
      field.name -> rawChecks.getOrElse(field.name, BsonArray())
    })
    val metricChecksBson = BsonDocument(metricChecks.map {
      case (metricName, check) =>
        metricName -> BsonDocument(
          "check" -> BsonBoolean(check.check),
          "description" -> BsonString(check.description),
          "threshold" -> BsonNumber(check.threshold),
          "value" -> BsonNumber(check.value),
          "metricSpecId" -> check.metricSpecId.map(BsonString.apply).getOrElse(BsonNull())
        )
    })

    val date = objectId.getValue.getDate()
    val calendar = new GregorianCalendar
    val bsonChecks = maybeBsonChecks.map(_.checks).getOrElse(Seq.empty) :+
      ("_hs_raw_checks" -> BsonDocument(allRawChecks.toMap)) :+
      ("_hs_metric_checks" -> metricChecksBson) :+
      ("_hs_latency" -> bsonLatency) :+
      ("_hs_error" -> bsonError) :+
      ("_hs_score" -> bsonScore) :+
      ("_hs_overall_score" -> overallScore) :+
      ("_hs_model_version_id" -> BsonNumber(modelVersion.id)) :+
      ("_hs_model_name" -> BsonString(modelVersion.name)) :+
      ("_hs_model_incremental_version" -> BsonNumber(modelVersion.version)) :+
      ("_hs_request_id" -> ei.metadata.map(_.requestId).map(BsonString.apply).getOrElse(BsonNull())) :+
      ("_hs_timestamp" -> BsonNumber(date.getTime)) :+
      ("_hs_year" -> BsonNumber(calendar.get(Calendar.YEAR))) :+
      ("_hs_month" -> BsonNumber(calendar.get(Calendar.MONTH) + 1)) :+
      ("_hs_day" -> BsonNumber(calendar.get(Calendar.DAY_OF_MONTH))) :+
      ("_id" -> objectId)

    val checksQuery: Document = Document(bsonChecks)

    val increments = maybeBsonChecks.map(_.aggregates).getOrElse(Seq.empty) ++ metricChecks.flatMap(check => Seq(s"_hs_metrics.${check._2.description}.checked" -> BsonNumber(1), s"_hs_metrics.${check._2.description}.passed" -> BsonNumber(check._2.check.toInt))) :+ ("_hs_requests" -> BsonNumber(1))
    val aggregatesQuery = Document(
      "$inc" -> increments,
      "$set" -> Document("_hs_last_id" -> objectId),
      "$push" -> Document("_hs_request_ids" -> objectId),
      "$setOnInsert" -> Document("_hs_first_id" -> objectId, "_hs_model_version_id" -> BsonNumber(modelVersion.id), "_hs_model_name" -> BsonString(modelVersion.name))
    )
    val aggregatesFilter: conversions.Bson = and(equal("_hs_model_version_id", modelVersion.id), lt("_hs_requests", modelVersion.monitoringConfiguration.map(_.batchSize).getOrElse(config.monitoring.batchSize)))
    
    val updateAggregatesEffect: F[Unit] = aggregatedCheckCollection.updateOne(aggregatesFilter, aggregatesQuery, (new UpdateOptions).upsert(true)).toFuture().liftToAsync[F].map(r => ())
    val insertCheckEffect: F[Unit] = checkCollection.insertOne(checksQuery).toFuture().liftToAsync[F].map(_ => ())
    
    updateAggregatesEffect *> insertCheckEffect
  }
}
