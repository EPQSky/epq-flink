package icu.epq.scalaexc

import icu.epq.scalaexc.Currency.Currency

class Money(val amount: Int, val currency: Currency) {
  override def toString = s"$amount $currency"
}
