package io.hydrosphere.sonar.terms

import akka.actor.typed.Behavior
import io.hydrosphere.sonar.actors.Processor
import shapeless._
import shapeless.labelled.FieldType

trait Processable[T] {
  def processor(t: T): Behavior[Processor.MetricMessage]
}

object Processable {

  trait ProcessableSyntax {
    def processor: Behavior[Processor.MetricMessage]
  }

  abstract class WrappedProcessable[Wrapped, Repr](implicit val tpe: Typeable[Wrapped]) {
    def processor(repr: Repr): Behavior[Processor.MetricMessage]
  }

  object implicits {
    implicit def processableSyntax[T](a: T)(implicit pr: Processable[T]): ProcessableSyntax = new ProcessableSyntax {
      override def processor: Behavior[Processor.MetricMessage] = pr.processor(a)
    }

    implicit def wrappedProcessableCNil[Wrapped](implicit tpe: Typeable[Wrapped]): WrappedProcessable[Wrapped, CNil] = new WrappedProcessable[Wrapped, CNil] {
      override def processor(repr: CNil): Behavior[Processor.MetricMessage] = throw new RuntimeException("Empty Coproduct")
    }

    implicit def wrappedProcessableCoproduct[Wrapped, Name <: Symbol, Instance, Remaining <: Coproduct](implicit tpe: Typeable[Wrapped], key: Witness.Aux[Name], processableHead: Lazy[Processable[Instance]], processableTail: Lazy[WrappedProcessable[Wrapped, Remaining]]): WrappedProcessable[Wrapped, FieldType[Name, Instance] :+: Remaining] = new WrappedProcessable[Wrapped, FieldType[Name, Instance] :+: Remaining] {
      override def processor(repr: FieldType[Name, Instance] :+: Remaining): Behavior[Processor.MetricMessage] = repr match {
        case Inl(head) => processableHead.value.processor(head)
        case Inr(tail) => processableTail.value.processor(tail)
      }
    }

    implicit def processableCoproduct[T, Repr <: Coproduct](implicit gen: LabelledGeneric.Aux[T, Repr], tpe: Typeable[T], wrappedProcessable: Lazy[WrappedProcessable[T, Repr]]): Processable[T] = (t: T) => wrappedProcessable.value.processor(gen.to(t))
  }

}