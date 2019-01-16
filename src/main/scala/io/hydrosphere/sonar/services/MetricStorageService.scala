package io.hydrosphere.sonar.services

import cats.effect.{Resource, Sync}
import cats.implicits._
import com.paulgoldbaum.influxdbclient.Parameter.Precision
import com.paulgoldbaum.influxdbclient.{Database, InfluxDB, Point}
import io.hydrosphere.sonar.Logging
import io.hydrosphere.sonar.config.Configuration
import io.hydrosphere.sonar.terms.Metric

import scala.concurrent.ExecutionContext.Implicits.global

trait MetricStorageService[F[_]] {
  def saveMetrics(metrics: Seq[Metric]): F[Unit]
}

class MetricStorageServiceDummyInterpreter[F[_]: Sync] extends MetricStorageService[F] with Logging {
  override def saveMetrics(metrics: Seq[Metric]): F[Unit] = {
    Sync[F].delay(logger.debug(s"Saving ${metrics.size} metrics"))
  }
}

class MetricStorageServiceInfluxInterpreter[F[_]: Sync](config: Configuration) extends MetricStorageService[F] with Logging {
  
  
  private def influx = {
    def acquire = Sync[F].delay(InfluxDB.connect(config.influx.host, config.influx.port))
    def release = (db: InfluxDB) => Sync[F].delay(db.close())

    Resource.make(acquire)(release)
  }
    
  private val database: Resource[F, Database] = {
    def acquire(server: InfluxDB) = Sync[F].delay(server.selectDatabase(config.influx.database))
    def release = (db: Database) => Sync[F].delay(db.close())
    
    for {
      server <- influx
      db <- Resource.make(acquire(server))(release)
    } yield db
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
      Sync[F].delay(db.bulkWrite(points, precision = Precision.MILLISECONDS))
    }
  }
}