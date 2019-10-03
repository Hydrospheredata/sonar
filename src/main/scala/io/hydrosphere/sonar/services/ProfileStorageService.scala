package io.hydrosphere.sonar.services

import cats.Monad
import cats.effect.concurrent.Ref
import cats.effect.{Async, Sync}
import cats.implicits._
import io.hydrosphere.sonar.Logging
import io.hydrosphere.sonar.config.Configuration
import io.hydrosphere.sonar.terms._
import io.hydrosphere.sonar.utils.FutureOps._
import io.hydrosphere.sonar.utils.math.{CountMinSketch, HyperLogLog}
import net.openhft.hashing.LongHashFunction
import org.bson.codecs.configuration.CodecRegistries.{fromCodecs, fromRegistries}
import org.bson.codecs.configuration.CodecRegistry
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.{BsonObjectId, Decimal128}
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.UpdateOptions
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase, model}

import scala.collection.JavaConverters._
import scala.math.BigDecimal.RoundingMode

trait ProfileStorageService[F[_]] {
  def saveProfile(profile: PreprocessedProfile, profileSourceKind: ProfileSourceKind): F[Unit]
  
  def batchSaveProfiles(profiles: Seq[PreprocessedProfile], profileSourceKind: ProfileSourceKind): F[Unit]

  def getProfile(modelVersionId: Long, fieldName: String, sourceKind: ProfileSourceKind): F[Option[Profile]]
  def getProfiles(modelVersionId: Long, sourceKind: ProfileSourceKind): F[Seq[Profile]]

  def getPreprocessedDistinctNames(modelVersionId: Long, sourceKind: ProfileSourceKind): F[Seq[String]]
}

class ProfileStorageServiceDummyInterpreter[F[_]: Sync] extends ProfileStorageService[F] with Logging {
  override def saveProfile(profile: PreprocessedProfile, profileSourceKind: ProfileSourceKind): F[Unit] = {
    Sync[F].delay(logger.debug(s"Saving profile: $profile"))
  }

  override def getProfile(modelVersionId: Long, fieldName: String, sourceKind: ProfileSourceKind): F[Option[Profile]] = {
    Sync[F].pure(None)
  }

  override def getPreprocessedDistinctNames(modelVersionId: Long, sourceKind: ProfileSourceKind): F[Seq[String]] =
    Sync[F].pure(Seq.empty)

  override def batchSaveProfiles(profiles: Seq[PreprocessedProfile], profileSourceKind: ProfileSourceKind): F[Unit] =
    Sync[F].unit

  override def getProfiles(modelVersionId: Long, sourceKind: ProfileSourceKind): F[Seq[Profile]] =
    Sync[F].pure(Seq.empty)
}

object ProfileStorageServiceMongoInterpreter {
  type IdNamePair = (Long, String)
  type ObjectIdState = Map[IdNamePair, BsonObjectId]

  class BigDecimalScalaCodec extends Codec[BigDecimal] {
    override def encode(writer: BsonWriter, value: BigDecimal, encoderContext: EncoderContext): Unit = {
      writer.writeDecimal128(new Decimal128(value.setScale(10, RoundingMode.CEILING).bigDecimal))
    }

    override def getEncoderClass: Class[BigDecimal] = classOf[BigDecimal]

    override def decode(reader: BsonReader, decoderContext: DecoderContext): BigDecimal = {
      reader.readDecimal128().bigDecimalValue()
    }
  }
}

import io.hydrosphere.sonar.services.ProfileStorageServiceMongoInterpreter._

class ProfileStorageServiceMongoInterpreter[F[_]: Async](config: Configuration, state: Ref[F, ObjectIdState], mongoClient: MongoClient) extends ProfileStorageService[F] with Logging {
  
  val codecRegistry: CodecRegistry = fromRegistries(
    fromCodecs(new BigDecimalScalaCodec),
    DEFAULT_CODEC_REGISTRY
  )
  
  lazy val database: MongoDatabase = mongoClient.getDatabase(config.mongo.database)
  
  def collection(profileSourceKind: ProfileSourceKind, database: MongoDatabase): MongoCollection[Document] =
    database.getCollection(s"${profileSourceKind.entryName}__preprocessed_profiles")
  
  def generateObjectId(modelVersionId: Long, name: String): BsonObjectId =
    BsonObjectId("%016X".format(LongHashFunction.xx().hashChars(name)) + "%08X".format(modelVersionId))

  def getObjectId(modelVersionId: Long, name: String): F[BsonObjectId] = for {
    st <- state.get
    maybeStored = st.get((modelVersionId, name))
    objectId <- maybeStored match {
      case Some(value) => Monad[F].pure(value)
      case None =>
        val objectId = generateObjectId(modelVersionId, name)
        state.modify(st => (st.updated((modelVersionId, name), objectId), objectId))
    }
  } yield objectId
  
  def numericalUpdateDocument(pp: NumericalPreprocessedProfile, objectId: BsonObjectId): Document = {
    val increment = Document(
      "sum" -> pp.sum.setScale(10, RoundingMode.CEILING),
      "size" -> pp.size,
      "squaresSum" -> pp.squaresSum.setScale(10, RoundingMode.CEILING),
      "fourthPowersSum" -> pp.fourthPowersSum.setScale(10, RoundingMode.CEILING),
      "missing" -> pp.missing
    )
    val selectMax = Document("max" -> pp.max)
    val selectMin = Document("min" -> pp.min)
    val histogramBinsDocument = Document(pp.histogramBins.toSeq.map({
      case (k, v) => (s"histogramBins.${k.toString.replace(".", "_")}", v)
    }))
    val cmsBucketsDocument = Document(pp.countMinSketch.buckets.toSeq.map({
      case (k, v) => (s"countMinSketch.buckets.$k", v)
    }))
    val hllBucketsDocument = Document(pp.hyperLogLog.buckets.toSeq.map({
      case (k, v) => (s"hyperLogLog.buckets.$k", v.toInt)
    }))
    
    val inc = increment ++ histogramBinsDocument ++ cmsBucketsDocument
    val max = selectMax ++ hllBucketsDocument
    Document(
      "$inc" -> inc,
      "$min" -> selectMin,
      "$max" -> max,
      "$setOnInsert" -> Document(
        "_id" -> objectId,
        "hyperLogLog.size" -> pp.hyperLogLog.size,
        "countMinSketch.size" -> pp.countMinSketch.size,
        "name" -> pp.name,
        "modelVersionId" -> pp.modelVersionId,
        "kind" -> "numerical"
      )
    )
  }
  
  def textUpdateDocument(pp: TextPreprocessedProfile, objectId: BsonObjectId): Document = {
    val increment = Document(
      "size" -> pp.size,
      "missing" -> pp.missing,
      "sentimentSum" -> pp.sentimentSum,
      "lengthSum" -> pp.lengthSum,
      "tokenLengthSum" -> pp.tokenLengthSum,
      "treeDepthSum" -> pp.treeDepthSum,
      "uniqueLemmasSum" -> pp.uniqueLemmasSum
    )
    val languagesSumDocument = Document(pp.languagesSum.toSeq.map({
      case (k, v) => (s"languagesSum.$k", v)
    }))
    val posTagSumDocument = Document(pp.posTagsSum.toSeq.map({
      case (k, v) => (s"posTagSum.$k", v)
    }))
    val hyperLogLogDocument = Document(pp.tokenHyperLogLog.buckets.toSeq.map({
      case (k, v) => (s"tokenHyperLogLog.buckets.$k", v)
    }))
    val inc = increment ++ languagesSumDocument ++ posTagSumDocument ++ hyperLogLogDocument
    Document(
      "$inc" -> inc,
      "$setOnInsert" -> Document(
        "_id" -> objectId,
        "tokenHyperLogLog.size" -> pp.tokenHyperLogLog.size,
        "name" -> pp.name,
        "modelVersionId" -> pp.modelVersionId,
        "kind" -> "text"
      )
    )
  }
  
  private def docToProfile(document: Document): Option[Profile] = Option(document).flatMap { doc =>
    doc.get("kind").map(_.asString().getValue).flatMap { x: String =>
      x match {
        case "numerical" => for {
          modelVersionId <- doc.get("modelVersionId").map(_.asInt64().longValue())
          name <- doc.get("name").map(_.asString().getValue)
          sum <- doc.get("sum").map(_.asDecimal128().decimal128Value().bigDecimalValue())
          size <- doc.get("size").map(_.asInt64().longValue())
          squaresSum <- doc.get("squaresSum").map(_.asDecimal128().decimal128Value().bigDecimalValue())
          fourthPowersSum <- doc.get("fourthPowersSum").map(_.asDecimal128().decimal128Value().bigDecimalValue())
          missing <- doc.get("missing").map(_.asInt64().longValue())
          min <- doc.get("min").map(_.asDouble().doubleValue())
          max <- doc.get("max").map(_.asDouble().doubleValue())
          histogramBins <- doc.get("histogramBins").map(_.asDocument().entrySet().asScala.map(e => e.getKey.replace("_", ".").toDouble -> e.getValue.asInt64().longValue()).toMap)
          hyperLogLog <- doc.get("hyperLogLog").map(d => Document(d.asDocument().toJson)).flatMap(hll => for {
            size <- hll.get("size").map(_.asInt32().intValue())
            buckets <- hll.get("buckets").map(_.asDocument().entrySet().asScala.map(e => e.getKey.toInt -> e.getValue.asInt32().intValue()).toMap)
          } yield HyperLogLog(size, buckets))
          countMinSketch <- doc.get("countMinSketch").map(d => Document(d.asDocument().toJson)).flatMap(cms => for {
            size <- cms.get("size").map(_.asInt32().intValue())
            buckets <- cms.get("buckets").map(_.asDocument().entrySet().asScala.map(e => e.getKey.toInt -> e.getValue.asInt64().longValue()).toMap)
          } yield CountMinSketch(size, buckets))
        } yield NumericalProfile(NumericalPreprocessedProfile(modelVersionId, name, sum, size, squaresSum, fourthPowersSum, missing, min, max, histogramBins, hyperLogLog, countMinSketch))
        case "text" => for {
          modelVersionId <- doc.get("modelVersionId").map(_.asInt64().longValue())
          name <- doc.get("name").map(_.asString().getValue)
          size <- doc.get("size").map(_.asInt64().longValue())
          missing <- doc.get("missing").map(_.asInt64().longValue())
          sentimentSum <- doc.get("sentimentSum").map(_.asInt64().longValue())
          lengthSum <- doc.get("lengthSum").map(_.asInt64().longValue())
          tokenLengthSum <- doc.get("tokenLengthSum").map(_.asInt64().longValue())
          treeDepthSum <- doc.get("treeDepthSum").map(_.asInt64().longValue())
          uniqueLemmaSum <- doc.get("uniqueLemmasSum").map(_.asDouble().doubleValue())
          //              languagesSum    <- doc.get("languagesSum").map(d => d.asDocument().entrySet().asScala.map(e => e.getKey -> e.getValue.asInt64().longValue()).toMap)
          posTagSum <- doc.get("posTagSum").map(d => d.asDocument().entrySet().asScala.map(e => e.getKey -> e.getValue.asInt64().longValue()).toMap)
          hyperLogLog <- doc.get("tokenHyperLogLog").map(d => Document(d.asDocument().toJson)).flatMap(hll => for {
            size <- hll.get("size").map(_.asInt32().intValue())
            buckets <- hll.get("buckets").map(_.asDocument().entrySet().asScala.map(e => e.getKey.toInt -> e.getValue.asInt32().intValue()).toMap)
          } yield HyperLogLog(size, buckets))
        } yield TextProfile(TextPreprocessedProfile(modelVersionId, name, size, missing, sentimentSum, lengthSum, tokenLengthSum, treeDepthSum, uniqueLemmaSum, Map.empty[String, Long], posTagSum, hyperLogLog))
      }
    }
  }
  
  def getProfile(modelVersionId: Long, fieldName: String, sourceKind: ProfileSourceKind): F[Option[Profile]] =
    collection(sourceKind, database)
      .find(and(equal("modelVersionId", modelVersionId), equal("name", fieldName)))
      .first()
      .toFuture()
      .liftToAsync[F]
      .map(docToProfile)

  override def getProfiles(modelVersionId: Long, sourceKind: ProfileSourceKind): F[Seq[Profile]] = {
    collection(sourceKind, database)
      .find(equal("modelVersionId", modelVersionId))
      .toFuture()
      .liftToAsync[F]
      .map(_.map(docToProfile).filter(_.isDefined).map(_.get))
  }
  
  def saveDocument(document: Document, objectId: BsonObjectId, profileSourceKind: ProfileSourceKind): F[Unit] =
    collection(profileSourceKind, database).updateOne(
      filter = Document("_id" -> objectId),
      update = document,
      options = (new UpdateOptions).upsert(true)
    ).toFuture().liftToAsync[F].map(_ => Unit)
  
  def batchSaveDocuments(documents: Seq[model.UpdateOneModel[Nothing]], profileSourceKind: ProfileSourceKind): F[Unit] =
    collection(profileSourceKind, database)
      .bulkWrite(documents)
      .toFuture()
      .liftToAsync[F]
      .map(_ => Unit)
  
  override def saveProfile(profile: PreprocessedProfile, profileSourceKind: ProfileSourceKind): F[Unit] = profile match {
    case p: NumericalPreprocessedProfile => for {
      objectId <- getObjectId(p.modelVersionId, p.name)
      updateDocument = numericalUpdateDocument(p, objectId)
      _ <- saveDocument(updateDocument, objectId, profileSourceKind)
    } yield Unit
    case p: TextPreprocessedProfile => for {
      objectId <- getObjectId(p.modelVersionId, p.name)
      updateDocument = textUpdateDocument(p, objectId)
      _ <- saveDocument(updateDocument, objectId, profileSourceKind)
    } yield Unit
  }


  override def batchSaveProfiles(profiles: Seq[PreprocessedProfile], profileSourceKind: ProfileSourceKind): F[Unit] = {
    implicitly[Sync[F]]
    val docsF = profiles.toList.map {
      case p: NumericalPreprocessedProfile => 
        Sync[F].suspend(
          getObjectId(p.modelVersionId, p.name)
            .map(objectId => model.UpdateOneModel(
              filter = Document("_id" -> objectId),
              update = numericalUpdateDocument(p, objectId),
              updateOptions = (new UpdateOptions).upsert(true)
            ))
        )
      case _ => ???
    }
    for {
      _ <- Sync[F].delay(logger.info(s"Saving batch profiles: ${profiles.size}"))
      docs <- docsF.sequence[F, model.UpdateOneModel[Nothing]]
      _ <- batchSaveDocuments(docs, profileSourceKind)
      _ <- Sync[F].delay(logger.info(s"${profiles.size} profiles was saved"))
    } yield Unit
  }

  override def getPreprocessedDistinctNames(modelVersionId: Long, sourceKind: ProfileSourceKind): F[Seq[String]] =
    collection(sourceKind, database)
      .distinct[String]("name", equal("modelVersionId", modelVersionId)).toFuture().liftToAsync[F]
}
