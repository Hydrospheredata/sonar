package io.hydrosphere.sonar.actors.processors.checks

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import cats.effect.IO
import io.hydrosphere.serving.manager.grpc.entities.ModelVersion
import io.hydrosphere.serving.monitoring.api.ExecutionInformation
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.services.ProfileStorageService
import io.hydrosphere.sonar.terms.{Check, Profile, ProfileSourceKind}

import scala.concurrent.duration._

//class ProfileCheckProcessor(context: ActorContext[Processor.CheckMessage])(implicit profileService: ProfileStorageService[IO]) extends AbstractBehavior[Processor.CheckMessage] with TimerScheduler {
//  override def onMessage(msg: Processor.CheckMessage): Behavior[Processor.CheckMessage] = msg match {
//    case Processor.CheckRequest(payload: ExecutionInformation) =>
//      payload.metadata match {
//        case Some(metadata) => 
//          profileService.getProfiles(metadata.modelVersionId, ProfileSourceKind.Training)
//          this
//        case None => this
//      }
////      profileService.getProfile()
////      this
//  }
//}
object ProfileCheckProcessor {

//  case object TimeKey
//  case object Timeout extends Processor.CheckMessage
//
//  def behavior(context: ActorContext[Processor.CheckMessage], modelVersion: ModelVersion)(implicit profileStorageService: ProfileStorageService[IO]): Behavior[Processor.CheckMessage] = {
//    Behaviors.withTimers { timers =>
//      timers.startPeriodicTimer(TimeKey, Timeout, 60.seconds)
//      active(context, modelVersion, Seq.empty)
//    }
//  }
//  
//  def active(context: ActorContext[Processor.CheckMessage], modelVersion: ModelVersion, buffer: Seq[Check], profiles: Seq[Profile])(implicit profileStorageService: ProfileStorageService[IO]): Behavior[Processor.CheckMessage] = {
//    Behaviors.receiveMessage {
//      case Processor.CheckRequest(info) => active(context, modelVersion, buffer, profiles)
//      case Timeout =>
//        context.log.info("Saving checks")
//        active(context, modelVersion, Seq.empty)
//    }
//  }
}