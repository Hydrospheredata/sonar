package io.hydrosphere.sonar.utils.checks

import io.hydrosphere.serving.monitoring.api.ExecutionInformation
import io.hydrosphere.sonar.Logging
import io.hydrosphere.sonar.terms.{Check, NumericalProfile, Profile}
import io.hydrosphere.sonar.utils.ExecutionInformationOps._

import scala.util.{Failure, Success, Try}

object ProfileChecks extends Logging {
  def check(profiles: Seq[Profile], request: ExecutionInformation): Map[String, Map[String, Check]] = Try {
    profiles.collect {
      case profile: NumericalProfile =>
        val idx :: name :: Nil = profile.name.reverse.split("_", 2).map(_.reverse).toList
        val input = Try {
          request.getDoubleInput(name)(idx.toInt)
        }.toOption
        val output = Try {
          request.getDoubleOutput(name)(idx.toInt)
        }.toOption
        val checks = (input ++ output).headOption match {
          case Some(value) => Map(
            "max" -> Check(value <= profile.quantileStatistics.max, "< max", value, profile.quantileStatistics.max),
            "min" -> Check(value >= profile.quantileStatistics.min, "> min", value, profile.quantileStatistics.min)
          )
          case None => Map.empty[String, Check]
        }
        name -> checks
    }.groupBy(_._1).mapValues(_.map(_._2).reduce(_ ++ _)).toMap
  } match {
    case Failure(exception) => 
      logger.error("error while computing checks", exception)
      Map.empty
    case Success(value) =>
      logger.info("checks were successfully computed")
      value
  }
}
