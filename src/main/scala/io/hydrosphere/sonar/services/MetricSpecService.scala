package io.hydrosphere.sonar.services

import cats.{Monad, MonadError}
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
import io.hydrosphere.sonar.terms.MetricSpec
import org.postgresql.util.PGobject

trait MetricSpecService[F[_]] {
  def createMetricSpec(metricSpec: MetricSpec): F[MetricSpec]
  def getMetricSpecById(id: String): F[MetricSpec]
  def getMetricSpecsByModelVersion(modelVersionId: Long): F[List[MetricSpec]]
  def getAllMetricSpecs: F[List[MetricSpec]]
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
    sql.query[MetricSpecFields].stream.map(toMetricSpec).compile.toList.transact(transactor)
  }

  override def getMetricSpecsByModelVersion(modelVersionId: Long): F[List[MetricSpec]] = {
    val sql =
      sql"""
           SELECT kind, name, modelVersionId, config, withHealth, id
           FROM metric_specs
           WHERE modelVersionId = $modelVersionId
         """
    sql.query[MetricSpecFields].stream.map(toMetricSpec).compile.toList.transact(transactor)
  }
}