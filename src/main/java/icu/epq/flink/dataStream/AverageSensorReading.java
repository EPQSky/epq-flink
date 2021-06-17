package icu.epq.flink.dataStream;

import icu.epq.flink.dataStream.utils.SensorSource;
import icu.epq.scala.entity.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 针对传感器数据流每5秒计算一次平均温度
 *
 * @author epqsky
 */
public class AverageSensorReading {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        int parallelism = environment.getParallelism();
        OutputTag<String> waring = new OutputTag<String>("waring-temp") {
        };
        SingleOutputStreamOperator<SensorReading> operator = environment.addSource(new SensorSource()).assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner((event, timestamp) -> event.timestamp()));
        SingleOutputStreamOperator<SensorReading> apply = operator.map(r -> new SensorReading(r.id(), r.timestamp(), (r.temperature() - 32) * (5.0 / 9.0)))
                .keyBy(SensorReading::id)
                .window(TumblingEventTimeWindows.of(Time.seconds(1L)))
                .apply(new TemperatureAverager());

        apply.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {
                if (sensorReading.temperature() > 32.0) {
                    context.output(waring, "当前温度过高：" + sensorReading.toString());
                }
            }
        }).getSideOutput(waring).print();
//        SingleOutputStreamOperator<String> ids = apply.map(SensorReading::id);
//        SingleOutputStreamOperator<String> flatMap = ids.flatMap((FlatMapFunction<String, String>) (value, out) -> {
//            for (String s : value.split("_")) {
//                out.collect(s);
//            }
//        }).returns(String.class);
//        flatMap.print();
        // apply.print();

        environment.execute("Compute average sensor temperature");
    }

    /**
     * User-defined WindowFunction to compute the average temperature of SensorReadings
     */
    public static class TemperatureAverager implements WindowFunction<SensorReading, SensorReading, String, TimeWindow> {

        /**
         * apply() is invoked once for each window.
         *
         * @param sensorId the key (sensorId) of the window
         * @param window   meta data for the window
         * @param input    an iterable over the collected sensor readings that were assigned to the window
         * @param out      a collector to emit results from the function
         */
        @Override
        public void apply(String sensorId, TimeWindow window, Iterable<SensorReading> input, Collector<SensorReading> out) {

            // compute the average temperature
            int cnt = 0;
            double sum = 0.0;
            for (SensorReading r : input) {
                cnt++;
                sum += r.temperature();
            }
            double avgTemp = sum / cnt;

            // emit a SensorReading with the average temperature
            out.collect(new SensorReading(sensorId, window.getEnd(), avgTemp));
        }
    }
}
