package io.hydrosphere.sonar.services

import cats.effect.Async
import cats.implicits._
import io.hydrosphere.sonar.config.Configuration
import org.mongodb.scala.{Document, MongoClient}
import io.hydrosphere.sonar.utils.mongo.ObservableExt._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.FindOneAndReplaceOptions


case class ModelSettings(modelVersionId: Long, batchSize: Long)

object ModelSettings {
  def default(modelVersionId: Long): ModelSettings = ModelSettings(
    modelVersionId = modelVersionId,
    batchSize = 100
  )
}

trait ModelSettingsService[F[_]] {
  def upsertSettings(modelSettings: ModelSettings): F[Unit]

  def getSettings(modelVersionId: Long): F[ModelSettings]
}

object ModelSettingsDocument {
  final val ModelVersionId = "model-version-id"
  final val BatchSize = "batch-size"

  def apply(modelSettings: ModelSettings): Document = {
    Document(
      ModelVersionId -> modelSettings.modelVersionId,
      BatchSize -> modelSettings.batchSize,
    )
  }

  def toSettings(document: Document): Option[ModelSettings] = {
    for {
      id <-  Option(document.getLong(ModelVersionId))
      batchSize <- Option(document.getLong(BatchSize))
    } yield ModelSettings(id, batchSize)
  }
}

class ModelSettingsServiceImpl[F[_]](configuration: Configuration, mongoClient: MongoClient)(implicit F: Async[F]) extends ModelSettingsService[F] {

  private val db = mongoClient.getDatabase(configuration.mongo.database)
  private val settingsCollection = db.getCollection("model_settings")

  override def upsertSettings(modelSettings: ModelSettings): F[Unit] = {
    val document = ModelSettingsDocument.apply(modelSettings)
    val options = FindOneAndReplaceOptions().upsert(true)
    settingsCollection.findOneAndReplace(
      equal(ModelSettingsDocument.ModelVersionId, modelSettings.modelVersionId),
      document,
      options
    ).toF[F].void
  }

  override def getSettings(modelVersionId: Long): F[ModelSettings] = {
    settingsCollection.find(equal(ModelSettingsDocument.ModelVersionId, modelVersionId)).toF[F]
      .map { list =>
        list.headOption
          .flatMap(ModelSettingsDocument.toSettings)
          .getOrElse(ModelSettings.default(modelVersionId))
      }
  }
}