package io.hydrosphere.sonar.services

import java.time.Instant

import cats.effect.{Async, Resource, Sync}
import cats.implicits._
import com.paulgoldbaum.influxdbclient.Parameter.Precision
import com.paulgoldbaum.influxdbclient.{Database, InfluxDB, Point, Record}
import io.hydrosphere.sonar.Logging
import io.hydrosphere.sonar.config.Configuration
import io.hydrosphere.sonar.terms.Metric
import io.hydrosphere.sonar.utils.FutureOps._

import scala.concurrent.ExecutionContext.Implicits.global

trait MetricStorageService[F[_]] {
  def saveMetrics(metrics: Seq[Metric]): F[Unit]
  
  def getMetrics(modelVersionId: Long, interval: Long, metrics: Seq[String], columnIndex: Option[String]): F[Seq[Metric]]
}

class MetricStorageServiceDummyInterpreter[F[_]: Sync] extends MetricStorageService[F] with Logging {
  override def saveMetrics(metrics: Seq[Metric]): F[Unit] = {
    Sync[F].delay(logger.debug(s"Saving ${metrics.size} metrics"))
  }

  override def getMetrics(modelVersionId: Long, interval: Long, metrics: Seq[String], columnIndex: Option[String]): F[Seq[Metric]] = 
    Sync[F].pure(Seq.empty)
}

class MetricStorageServiceInfluxInterpreter[F[_]: Async](config: Configuration) extends MetricStorageService[F] with Logging {
  
  private def influx = {
    def acquire = Async[F].delay(InfluxDB.connect(config.influx.host, config.influx.port))
    def release = (db: InfluxDB) => Async[F].delay(db.close())

    Resource.make(acquire)(release)
  }
  
  private def createDatabaseIfNotExist(database: Database): F[Database] = for {
    exists <- database.exists().liftToAsync[F]
    _ <- if (exists)
            Async[F].unit
         else
            database.create().liftToAsync[F]
  } yield database
    
  private val database: Resource[F, Database] = {
    def acquire(server: InfluxDB) = createDatabaseIfNotExist(server.selectDatabase(config.influx.database))
    def release = (_: Database) => Async[F].delay(logger.debug("Closing database"))
    
    for {
      server <- influx
      db <- Resource.make(acquire(server))(release)
    } yield db
  }

  private def metricFromRecord(name: String, record: Record): Metric = {
    Metric(
      name = name, 
      value = record("value").asInstanceOf[BigDecimal].toDouble, 
      labels = Map("modelVersionId" -> record("modelVersionId").toString),
      health = Option(record("health")).map(_.asInstanceOf[Boolean]),
      timestamp = Instant.parse(record("time").toString).getEpochSecond
    )
  }

  override def getMetrics(modelVersionId: Long, interval: Long, metrics: Seq[String], columnIndex: Option[String]): F[Seq[Metric]] = {
    val columnIndexClause = columnIndex.map(ci => s""" AND "columnIndex" = '$ci' """).getOrElse("")
    
    val query = s"""SELECT "value", "health", "modelVersionId"::tag, "columnIndex"::tag FROM ${metrics.mkString(",")} WHERE "modelVersionId" = '$modelVersionId' AND time >= now() - ${interval / 60000}m $columnIndexClause"""
    
    database.use { db =>
      db.query(query)
        .liftToAsync[F]
        .map(queryResult => for {
          s <- queryResult.series
          r <- s.records
        } yield metricFromRecord(s.name, r))
    }
  }

  override def saveMetrics(metrics: Seq[Metric]): F[Unit] = {
    val points = metrics.map { metric =>
      var point = Point(metric.name, metric.timestamp)
        .addField("value", metric.value)
      point = metric.health match {
        case Some(h) => point.addField("health", if (h) 1 else 0)
        case None => point // pass
      }
      metric.labels.foreach({ case (k, v) =>
        if (v != "" && v != null) {
          point = point.addTag(k, v)
        }
      })
      point
    }
    database.use { db =>
      db.bulkWrite(points, precision = Precision.MILLISECONDS).liftToAsync[F].map(_ => Unit)
    }
  }
}