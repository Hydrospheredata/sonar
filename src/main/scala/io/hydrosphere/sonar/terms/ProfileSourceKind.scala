package io.hydrosphere.sonar.terms

import enumeratum._

import scala.collection.immutable

sealed trait ProfileSourceKind extends EnumEntry

object ProfileSourceKind extends Enum[ProfileSourceKind] {
  val values: immutable.IndexedSeq[ProfileSourceKind] = findValues
  
  case object Training extends ProfileSourceKind
  case object Production extends ProfileSourceKind
}
