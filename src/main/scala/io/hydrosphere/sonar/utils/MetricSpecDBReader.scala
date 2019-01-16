package io.hydrosphere.sonar.utils

import shapeless._
import shapeless.labelled.{FieldType, field}
import shapeless.record._

object MetricSpecDBReader {
  
  type MetricSpecFields = Record.`'kind -> String, 'name -> String, 'modelVersionId -> Long, 'config -> io.circe.Json, 'withHealth -> Boolean, 'id -> String`.T
  type Rec[S] = String :: Long :: S :: Boolean :: String :: HNil

  trait DBReader[T] {
    def read(rec: MetricSpecFields): T
  }

  abstract class WrappedDBReader[Wrapped, SubRepr](implicit tpe: Typeable[Wrapped]) {
    def readRec(rec: MetricSpecFields): SubRepr
  }
  
  implicit def hNilReader[Wrapped](implicit tpe: Typeable[Wrapped]): WrappedDBReader[Wrapped, HNil] = new WrappedDBReader[Wrapped, HNil] {
    override def readRec(rec: MetricSpecFields): HNil = HNil
  }

  implicit def hListReader[Wrapped, Repr](implicit tpe: Typeable[Wrapped], decoder: io.circe.Decoder[Repr]): WrappedDBReader[Wrapped, Rec[Repr]] = new WrappedDBReader[Wrapped, Rec[Repr]] {
    override def readRec(rec: MetricSpecFields): Rec[Repr] = rec.get('config).as[Repr] match {
      case Right(x) => rec.get('name) :: rec.get('modelVersionId) :: x :: rec.get('withHealth) :: rec.get('id) :: HNil
      case Left(e) => throw new RuntimeException(s"Can't parse json config: $e")
    }
  }
  
  implicit def cNilFormat[Wrapped](implicit t: Typeable[Wrapped]): WrappedDBReader[Wrapped, CNil] = new WrappedDBReader[Wrapped, CNil] {
    override def readRec(rec: MetricSpecFields): CNil = throw new Exception("wtf")
  }

  implicit def cListReader[Wrapped, Name <: Symbol, Instance, Remaining <: Coproduct](implicit tpe: Typeable[Wrapped], key: Witness.Aux[Name], fh: Lazy[DBReader[Instance]], ft: Lazy[WrappedDBReader[Wrapped, Remaining]]): WrappedDBReader[Wrapped, FieldType[Name, Instance] :+: Remaining] = new WrappedDBReader[Wrapped, FieldType[Name, Instance] :+: Remaining] {
    override def readRec(rec: MetricSpecFields): FieldType[Name, Instance] :+: Remaining = {
      if (key.value.name == rec.head) {
        Inl(field[Name](fh.value.read(rec)))
      } else {
        Inr(ft.value.readRec(rec))
      }
    }
  }

  implicit def hlistreader[T <: Product, Repr <: HList](implicit gen: Generic.Aux[T, Repr], sg: Lazy[WrappedDBReader[T, Repr]], tpe: Typeable[T]): DBReader[T] = new DBReader[T] {
    override def read(rec: MetricSpecFields): T = {
      gen.from(sg.value.readRec(rec))
    }
  }

  implicit def reader[T, Repr <: Coproduct](implicit gen: LabelledGeneric.Aux[T, Repr], sg: Lazy[WrappedDBReader[T, Repr]], tpe: Typeable[T]): DBReader[T] = new DBReader[T] {
    override def read(rec: MetricSpecFields): T = {
      gen.from(sg.value.readRec(rec))
    }
  }

  implicit class EnrichDBFields(fields: MetricSpecFields) {
    def toADT[T](implicit f: DBReader[T]): T = f.read(fields)
  }
}
