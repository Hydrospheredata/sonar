package io.hydrosphere.sonar.services

import java.time.Instant
import java.util.Date

import cats.effect.{Async, Resource, Sync}
import cats.implicits._
import com.paulgoldbaum.influxdbclient.Parameter.Precision
import com.paulgoldbaum.influxdbclient._
import io.hydrosphere.sonar.Logging
import io.hydrosphere.sonar.config.Configuration
import io.hydrosphere.sonar.terms.{Metric, MetricLabels, MetricsAggregation}
import io.hydrosphere.sonar.utils.FutureOps._
import io.circe._
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.generic.auto._
import io.hydrosphere.serving.monitoring.metadata.TraceData
import org.joda.time.format.ISODateTimeFormat

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

trait MetricStorageService[F[_]] {
  def saveMetrics(metrics: Seq[Metric]): F[Unit]

  def getMetrics(modelVersionId: Long, interval: Long, metrics: Seq[String], metricSpecId: String, columnIndex: Option[String]): F[Seq[Metric]]

  def getMetricsAggregationRange(modelVersionId: Long,
                                 metrics: Seq[String],
                                 from: Option[Long] = None,
                                 till: Option[Long] = None,
                                 steps: Int = 50): F[Seq[MetricsAggregation]]

  def getMetricsRange(modelVersionId: Long, from: Long, till:Long, metrics: Seq[String], columnIndex: Option[String], health: Option[Int]): F[Seq[Metric]]
}

class MetricStorageServiceDummyInterpreter[F[_] : Sync] extends MetricStorageService[F] with Logging {
  override def saveMetrics(metrics: Seq[Metric]): F[Unit] = {
    Sync[F].delay(logger.debug(s"Saving ${metrics.size} metrics"))
  }

  override def getMetrics(modelVersionId: Long, interval: Long, metrics: Seq[String], metricSpecId: String, columnIndex: Option[String]): F[Seq[Metric]] =
    Sync[F].pure(Seq.empty)

  override def getMetricsAggregationRange(modelVersionId: Long, metrics: Seq[String], from: Option[Long], till: Option[Long], steps: Int): F[Seq[MetricsAggregation]] = Sync[F].pure(Seq.empty)

  override def getMetricsRange(modelVersionId: Long, from: Long, till: Long, metrics: Seq[String], columnIndex: Option[String], health: Option[Int]): F[Seq[Metric]] = Sync[F].pure(Seq.empty)
}

class MetricStorageServiceInfluxInterpreter[F[_] : Async](config: Configuration) extends MetricStorageService[F] with Logging {

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
      labels = MetricLabels(
        modelVersionId = record("modelVersionId").toString.toLong,
        metricSpecId = Try(record("metricSpecId").toString).getOrElse(""),
        traces = (for {
          json <- Try(record("traces").toString)
          parsed <- decode[Seq[Option[TraceData]]](json).toTry
        } yield parsed).getOrElse(Seq.empty[Option[TraceData]]),
        originTraces = (for {
          json <- Try(record("originTraces").toString)
          parsed <- decode[Seq[Option[TraceData]]](json).toTry
        } yield parsed).getOrElse(Seq.empty[Option[TraceData]]),
        columnIndex = Try(record("columnIndex").toString.toInt).toOption
      ),
      health = Option(record("health")).map(_.toString == "1"),
      timestamp = Instant.parse(record("time").toString).toEpochMilli
    )
  }

  private val dateFormater = ISODateTimeFormat.dateTimeNoMillis()


  private def toDate(epochSec:Long):String = dateFormater.print(epochSec * 1000L)

  sealed trait Bounds
  sealed case class Upper() extends Bounds
  sealed case class Lower() extends Bounds

  private def findBound(modelVersionId: Long, metrics: Seq[String], bound:Bounds):F[String] = {

    val (defaultDate:String, selector:String) = bound match {
      case Upper() => (dateFormater.print(new Date().getTime), "last(value)" )
      case Lower() => (dateFormater.print(0L), "first(value)" )
      case _ => throw new RuntimeException(s"wtf: what are you? $bound")
    }

    val query = s"""SELECT time, $selector FROM ${metrics.mkString(",")}  WHERE modelVersionId = '$modelVersionId' """

    database.use{ db =>

      db.query(query)
        .liftToAsync[F]
        .map{queryResult =>
          val maybeTime = for {
            s <- queryResult.series.headOption
            r <- s.records.headOption
          } yield r("time").toString



          maybeTime.getOrElse(defaultDate)
        }
    }
  }

  private def recordsToReports(qr:QueryResult, modelVersionId:Long, stepInMS:Long) = for {
    s <- qr.series
    r <- s.records
  } yield {

    val fromTs = Instant.parse(r("time").toString).toEpochMilli

    MetricsAggregation(
      meanValue = Option(r("meanValue")).map(_.asInstanceOf[BigDecimal].toDouble),
      meanHealth = Option(r("meanHealth")).map(_.asInstanceOf[BigDecimal].toDouble),
      from = fromTs,
      till = fromTs + stepInMS,
      modelVersionId = modelVersionId,
      minValue = Option(r("minValue")).map(_.asInstanceOf[BigDecimal].toDouble),
      maxValue = Option(r("maxValue")).map(_.asInstanceOf[BigDecimal].toDouble)
    )
  }

  override def getMetricsAggregationRange(modelVersionId: Long,
                                          metrics: Seq[String],
                                          from: Option[Long] = None,
                                          till: Option[Long] = None,
                                          _steps: Int = 50): F[Seq[MetricsAggregation]]= {

    val steps = if(_steps <= 2) 1 else  _steps - 1

    val fromF:F[String] = from.map(seconds => Sync[F].pure(toDate(seconds))).getOrElse(findBound(modelVersionId, metrics, Lower()))
    val tillF:F[String] = till.map(seconds => Sync[F].pure(toDate(seconds))).getOrElse(findBound(modelVersionId, metrics, Upper()))

    for {
      fromDT <- fromF
      tillDT <- tillF
      step = Math.ceil((Instant.parse(tillDT).toEpochMilli  - Instant.parse(fromDT).toEpochMilli) / steps).longValue()
      q = s"""SELECT mean("health") as meanHealth, mean("value") as meanValue, min("value") as minValue, max("value") as maxValue
                 |FROM ${metrics.mkString(",")}
                 |WHERE time >= '${fromDT}' AND time <= '${tillDT}' AND "modelVersionId" = '${modelVersionId}'
                 |GROUP BY  time(${step}ms)"""
        .stripMargin
      result <- database.use{ _.query(q).liftToAsync[F].map{ recordsToReports(_, modelVersionId, step)}}
    } yield result

  }

  override def getMetricsRange(modelVersionId: Long, from: Long, till:Long, metrics: Seq[String], columnIndex: Option[String], health: Option[Int]):F[Seq[Metric]] = {

    val columnIndexClause = columnIndex.map(ci => s""" AND "columnIndex" = '$ci' """).getOrElse("")

    val healthClause = health.map(healthVal => s" AND health = $healthVal ").getOrElse("")

    val query =
      s"""SELECT "value", "health", "modelVersionId"::tag, "columnIndex"::tag, "traces"::tag, "trace"::tag
         |FROM ${metrics.mkString(",")}
         |WHERE "modelVersionId" = '$modelVersionId'
         |AND time >= '${toDate(from)}' AND time <= '${toDate(till)}' $columnIndexClause $healthClause""".stripMargin

    database.use { db =>
      db.query(query)
        .liftToAsync[F]
        .map(queryResult => for {
          s <- queryResult.series
          r <- s.records
        } yield metricFromRecord(s.name, r))
    }

  }

  override def getMetrics(modelVersionId: Long, interval: Long, metrics: Seq[String],metricSpecId: String, columnIndex: Option[String]): F[Seq[Metric]] = {
    val columnIndexClause = columnIndex.map(ci => s""" AND "columnIndex" = '$ci' """).getOrElse("")

    val query =
      s"""SELECT "value", "health", "metricSpecId"::tag, "modelVersionId"::tag, "columnIndex"::tag, "traces"::tag, "originTraces"::tag 
         |FROM ${metrics.mkString(",")} 
         |WHERE "modelVersionId" = '$modelVersionId' AND time >= now() - ${interval / 60000}m AND "metricSpecId" = '$metricSpecId' $columnIndexClause""".stripMargin
    
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
      point = point
        .addTag("metricSpecId", metric.labels.metricSpecId)
        .addTag("modelVersionId", metric.labels.modelVersionId.toString)
        .addTag("traces", metric.labels.traces.asJson.noSpaces)
        .addTag("originTraces", metric.labels.originTraces.asJson.noSpaces)
      if (metric.labels.columnIndex.nonEmpty) {
        point = point.addTag("columnIndex", metric.labels.columnIndex.get.toString)
      }

      point
    }
    database.use { db =>
      db.bulkWrite(points, precision = Precision.MILLISECONDS).liftToAsync[F].map(_ => Unit)
    }
  }
}