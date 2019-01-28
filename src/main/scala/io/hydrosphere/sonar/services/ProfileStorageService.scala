package io.hydrosphere.sonar.services

import cats.Monad
import cats.effect.concurrent.Ref
import cats.effect.{Async, Resource, Sync}
import cats.implicits._
import com.mongodb.MongoCredential.createCredential
import com.mongodb.{ConnectionString, MongoCredential}
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
import org.mongodb.scala.model.UpdateOptions
import org.mongodb.scala.{Document, MongoClient, MongoClientSettings, MongoCollection, MongoDatabase}
import org.mongodb.scala.model.Filters._

import scala.collection.JavaConverters._
import scala.math.BigDecimal.RoundingMode

trait ProfileStorageService[F[_]] {
  def saveProfile(profile: PreprocessedProfile, profileSourceKind: ProfileSourceKind): F[Unit]

  def getProfile(modelVersionId: Long, fieldName: String, sourceKind: ProfileSourceKind): F[Option[Profile]]

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

class ProfileStorageServiceMongoInterpreter[F[_]: Async](config: Configuration, state: Ref[F, ObjectIdState]) extends ProfileStorageService[F] {
  
  val codecRegistry: CodecRegistry = fromRegistries(
    fromCodecs(new BigDecimalScalaCodec),
    DEFAULT_CODEC_REGISTRY
  )
  
  val mongoClient: Resource[F, MongoClient] = {
    def acquire = Sync[F].delay {
      val builder = MongoClientSettings
        .builder()
        .applyToClusterSettings(b => b.applyConnectionString(new ConnectionString(s"mongodb://${config.mongo.host}:${config.mongo.port}/${config.mongo.database}?authSource=admin")))
        .applyToConnectionPoolSettings(b => b.maxWaitQueueSize(1000).maxSize(200))
      val credentials: Option[MongoCredential] = for {
        user <- config.mongo.user
        pass <- config.mongo.pass
        authDb <- config.mongo.authDb
      } yield createCredential(user, authDb, pass.toCharArray)

      credentials match {
        case Some(creds) => builder.credential(creds)
        case None =>
      }

      val settings = builder.build()
      MongoClient(settings)
    }
    def release = (client: MongoClient) => Sync[F].delay(client.close())
    
    Resource.make(acquire)(release)
  }
  
  def database(client: MongoClient): MongoDatabase = client.getDatabase(config.mongo.database)
  
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
  
  def getProfile(modelVersionId: Long, fieldName: String, sourceKind: ProfileSourceKind): F[Option[Profile]] = mongoClient.use { client =>
    collection(sourceKind, database(client))
      .find(and(equal("modelVersionId", modelVersionId), equal("name", fieldName)))
      .first()
      .toFuture()
      .liftToAsync[F]
      .map(doc => {
        if (doc == null) {
          None
        } else {
          doc.get("kind").map(_.asString().getValue).flatMap { x: String => x match {
            case "numerical" => for {
              modelVersionId  <- doc.get("modelVersionId").map(_.asInt64().longValue())
              name            <- doc.get("name").map(_.asString().getValue)
              sum             <- doc.get("sum").map(_.asDecimal128().decimal128Value().bigDecimalValue())
              size            <- doc.get("size").map(_.asInt64().longValue())
              squaresSum      <- doc.get("squaresSum").map(_.asDecimal128().decimal128Value().bigDecimalValue())
              fourthPowersSum <- doc.get("fourthPowersSum").map(_.asDecimal128().decimal128Value().bigDecimalValue())
              missing         <- doc.get("missing").map(_.asInt64().longValue())
              min             <- doc.get("min").map(_.asDouble().doubleValue())
              max             <- doc.get("max").map(_.asDouble().doubleValue())
              histogramBins   <- doc.get("histogramBins").map(_.asDocument().entrySet().asScala.map(e => e.getKey.replace("_", ".").toDouble -> e.getValue.asInt64().longValue()).toMap)
              hyperLogLog     <- doc.get("hyperLogLog").map(d => Document(d.asDocument().toJson)).flatMap(hll => for {
                size <- hll.get("size").map(_.asInt32().intValue())
                buckets <- hll.get("buckets").map(_.asDocument().entrySet().asScala.map(e => e.getKey.toInt -> e.getValue.asInt32().intValue()).toMap)
              } yield HyperLogLog(size, buckets))
              countMinSketch <- doc.get("countMinSketch").map(d => Document(d.asDocument().toJson)).flatMap(cms => for {
                size <- cms.get("size").map(_.asInt32().intValue())
                buckets <- cms.get("buckets").map(_.asDocument().entrySet().asScala.map(e => e.getKey.toInt -> e.getValue.asInt64().longValue()).toMap)
              } yield CountMinSketch(size, buckets))
            } yield NumericalProfile(NumericalPreprocessedProfile(modelVersionId, name, sum, size, squaresSum, fourthPowersSum, missing, min, max, histogramBins, hyperLogLog, countMinSketch))
          }}
        }
      })
  }
  
  def saveDocument(document: Document, objectId: BsonObjectId, profileSourceKind: ProfileSourceKind): F[Unit] = mongoClient.use { client =>
    collection(profileSourceKind, database(client)).updateOne(
      filter = Document("_id" -> objectId),
      update = document,
      options = (new UpdateOptions).upsert(true)
    ).toFuture().liftToAsync[F].map(_ => Unit)
  }
  
  override def saveProfile(profile: PreprocessedProfile, profileSourceKind: ProfileSourceKind): F[Unit] = profile match {
    case p: NumericalPreprocessedProfile => for {
      objectId <- getObjectId(p.modelVersionId, p.name)
      updateDocument = numericalUpdateDocument(p, objectId)
      _ <- saveDocument(updateDocument, objectId, profileSourceKind)
    } yield Unit
    case _: TextPreprocessedProfile => ???
  }

  override def getPreprocessedDistinctNames(modelVersionId: Long, sourceKind: ProfileSourceKind): F[Seq[String]] = mongoClient.use { client =>
    collection(sourceKind, database(client))
      .distinct[String]("name", equal("modelVersionId", modelVersionId)).toFuture().liftToAsync[F]
  }
}