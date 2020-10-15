package io.hydrosphere.sonar.common.utils.checks

import io.hydrosphere.serving.monitoring.api.ExecutionInformation
import io.hydrosphere.sonar.common.terms.{Check, NumericalProfile, Profile}
import io.hydrosphere.sonar.common.utils.Logging
import io.hydrosphere.sonar.common.terms.Check
import io.hydrosphere.sonar.common.utils.ExecutionInformationOps._

import scala.util.{Failure, Success, Try}

object ProfileChecks extends Logging {
  def check(profiles: Seq[Profile], request: ExecutionInformation): Map[String, Seq[Check]] = Try {
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
          case Some(value) => Seq(
            Check(value <= profile.quantileStatistics.max, "< max", value, profile.quantileStatistics.max),
            Check(value >= profile.quantileStatistics.min, "> min", value, profile.quantileStatistics.min)
          )
          case None => Seq.empty[Check]
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
