package icu.epq.scala

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlinkTest {

  def main(args: Array[String]): Unit = {
    split()
  }

  def reduce(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream: DataStream[(String, List[String])] = env.fromElements(("en", List("tea")), ("fr", List("vin")), ("en", List("cake")))
    val resultStream: DataStream[(String, List[String])] = inputStream.keyBy(map => map._1).reduce((x, y) => (x._1, x._2 ::: y._2))
    resultStream.print()
    env.execute()
  }

  def split(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputSteam: DataStream[(Int, String)] = env.fromElements((1, "one"), (2, "two"), (1000, "one thousand"), (2000, "two thousand"), (10000, "ten thousand"))
    val large: OutputTag[(Int, String)] = new OutputTag[(Int, String)]("large")
    val small: OutputTag[(Int, String)] = new OutputTag[(Int, String)]("small")

    val slitted: DataStream[(Int, String)] = inputSteam.process[(Int, String)]((i: (Int, String), context: ProcessFunction[(Int, String), (Int, String)]#Context, _: Collector[(Int, String)]) => {
      if (i._1 > 1000) context.output(large, i) else context.output(small, i)
    })

    slitted.getSideOutput(large).print
    env.execute()
  }

}
