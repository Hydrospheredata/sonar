package io.hydrosphere.sonar.legacy.actors.writers

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import cats.effect.IO
import io.hydrosphere.sonar.common.terms.{PreprocessedProfile, ProfileSourceKind}
import io.hydrosphere.sonar.legacy.services.ProfileStorageService

object ProfileWriter {
  sealed trait Message
  final case class ProcessedProfile(profile: PreprocessedProfile) extends Message
  
  def apply(profileStorageService: ProfileStorageService[IO]): Behavior[Message] = Behaviors.setup { ctx => new ProfileWriter(ctx, profileStorageService) }
}

class ProfileWriter(context: ActorContext[ProfileWriter.Message], profileStorageService: ProfileStorageService[IO]) extends AbstractBehavior[ProfileWriter.Message] {
  override def onMessage(msg: ProfileWriter.Message): Behavior[ProfileWriter.Message] = msg match {
    case ProfileWriter.ProcessedProfile(profile) =>
      profileStorageService.saveProfile(profile, ProfileSourceKind.Production).unsafeRunAsync {
        case Right(_) => context.log.debug(s"Written profile")
        case Left(exc) => context.log.error(exc, s"Error while writing profile $profile")
      }
      this
  }
}