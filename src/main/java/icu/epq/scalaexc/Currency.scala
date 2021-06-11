package icu.epq.scalaexc

object Currency extends Enumeration {
  type Currency = Value
  val CNY, GBP, INR, JPY, NOK, PLN, SEK, USD = Value
}