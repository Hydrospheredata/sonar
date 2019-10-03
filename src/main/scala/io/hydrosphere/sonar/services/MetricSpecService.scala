package io.hydrosphere.sonar.services

import cats.implicits._
import cats.effect._
import doobie._
import doobie.implicits._
import io.circe.Json
import io.circe.parser.parse
import io.circe.syntax._
import io.circe.generic.extras.auto._
import io.circe.generic.extras.{Configuration => CirceExtraConfiguration}
import io.hydrosphere.sonar.utils.{MetricSpecDBReader, MetricSpecNotFound}
import io.hydrosphere.sonar.terms._
import org.postgresql.util.PGobject

trait MetricSpecService[F[_]] {
  
  def defaultMetricSpec(modelVersionId: Long): List[MetricSpec] = List(
    LatencyMetricSpec("Latency", modelVersionId, HealthRateMetricSpecConfiguration(30, None), false, "fake-id-latency"),
    CounterMetricSpec("Request Rate", modelVersionId, CounterMetricSpecConfiguration(30), false, "fake-id-counter"),
    ErrorRateMetricSpec("Error Rate", modelVersionId, HealthRateMetricSpecConfiguration(30, None), false, "fake-id-error-rate")
  )
  
  def createMetricSpec(metricSpec: MetricSpec): F[MetricSpec]
  def getMetricSpecById(id: String): F[MetricSpec]
  def getMetricSpecsByModelVersion(modelVersionId: Long): F[List[MetricSpec]]
  def getCustomMetricSpecByModelVersion(modelVersionId: Long): F[List[MetricSpec]]
  def getAllMetricSpecs: F[List[MetricSpec]]
  def remove(id: String): F[Unit]
}

class MetricSpecServiceInterpreter[F[_] : Sync](transactor: Transactor[F]) extends MetricSpecService[F] {

  import MetricSpecDBReader._

  implicit val genDevConfig: CirceExtraConfiguration =
    CirceExtraConfiguration.default.withDefaults.withDiscriminator("kind")

  implicit val configurationMeta: Meta[Json] =
    Meta.Advanced.other[PGobject]("json").timap[Json](
      a => parse(a.getValue) match {
        case Left(e) => throw e
        case Right(conf) => conf
      }
    )(
      a => {
        val o = new PGobject
        o.setType("json")
        o.setValue(a.noSpaces)
        o
      }
    )

  private def kind(metricSpec: MetricSpec): String = {
    metricSpec.getClass.getSimpleName
  }

  private def toMetricSpec(fields: MetricSpecFields): MetricSpec = fields.toADT[MetricSpec]

  override def createMetricSpec(metricSpec: MetricSpec): F[MetricSpec] = {
    import metricSpec._
    sql"""
         INSERT INTO metric_specs(kind, name, modelVersionId, config, withHealth, id)
         VALUES (${kind(metricSpec)}, $name, $modelVersionId, ${metricSpec.config.asJson}, $withHealth, $id)
         ON CONFLICT (id) DO UPDATE
         SET kind = ${kind(metricSpec)},
             name = $name,
             modelVersionId = $modelVersionId,
             config = ${metricSpec.config.asJson},
             withHealth = $withHealth
       """.update.run.map(_ => metricSpec).transact(transactor)
  }

  override def getMetricSpecById(id: String): F[MetricSpec] = {
    val sql =
      sql"""
         SELECT kind, name, modelVersionId, config, withHealth, id
         FROM metric_specs
         WHERE id = $id
       """
    sql.query[MetricSpecFields].option.transact(transactor).flatMap {
      case Some(fields) => Sync[F].pure(toMetricSpec(fields))
      case None => Sync[F].raiseError(MetricSpecNotFound(id))
    }
  }

  override def getAllMetricSpecs: F[List[MetricSpec]] = {
    val sql =
      sql"""
           SELECT kind, name, modelVersionId, config, withHealth, id
           FROM metric_specs
         """
    sql
      .query[MetricSpecFields]
      .stream
      .map(toMetricSpec)
      .compile
      .toList
      .map(metricSpecs => metricSpecs ++ metricSpecs.map(_.modelVersionId).distinct.flatMap(mvId => defaultMetricSpec(mvId)))
      .transact(transactor)
  }

  override def getMetricSpecsByModelVersion(modelVersionId: Long): F[List[MetricSpec]] = {
    val sql =
      sql"""
           SELECT kind, name, modelVersionId, config, withHealth, id
           FROM metric_specs
           WHERE modelVersionId = $modelVersionId
         """
    sql
      .query[MetricSpecFields]
      .stream
      .map(toMetricSpec)
      .compile
      .toList
      .map(_ ++ defaultMetricSpec(modelVersionId))
      .transact(transactor)
  }
  
  override def getCustomMetricSpecByModelVersion(modelVersionId: Long): F[List[MetricSpec]] = {
    val sql =
      sql"""
           SELECT kind, name, modelVersionId, config, withHealth, id
           FROM metric_specs
           WHERE modelVersionId = $modelVersionId AND kind = 'CustomModelMetricSpec'
         """
    println(sql, modelVersionId)
    sql
      .query[MetricSpecFields]
      .stream
      .map(toMetricSpec)
      .compile
      .toList
      .transact(transactor)
  }

  override def remove(id: String): F[Unit] = {
    val sql =
      sql"""
           DELETE FROM metric_specs WHERE id = $id
         """
    sql.update.run.map(_ => ()).transact(transactor)
  }
}