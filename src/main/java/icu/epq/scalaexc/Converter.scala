package icu.epq.scalaexc

import icu.epq.scalaexc.Currency.Currency

object Converter {
  def convert(money: Money, to: Currency): Money = {
    // 获取当前的市场汇率……这里使用了模拟值
    val conversionRate = 2
    new Money(money.amount * conversionRate, to)
  }
}
