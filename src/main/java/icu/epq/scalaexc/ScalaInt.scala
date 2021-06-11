package icu.epq.scalaexc3

import java.util

/**
 * 所有类型都视为对象
 */
class ScalaInt {

  def main(args: Array[String]): Unit = {
    playWithInt()
  }

  def playWithInt(): Unit = {
    val capacity: Int = 10
    val list = new util.ArrayList[String]
    list.ensureCapacity(capacity)
  }
}
