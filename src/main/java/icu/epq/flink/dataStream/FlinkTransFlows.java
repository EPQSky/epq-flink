package icu.epq.flink.dataStream;

import icu.epq.flink.dataStream.utils.SensorSource;
import icu.epq.scala.entity.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * 流合并转换
 *
 * @author epqsky
 */
public class FlinkTransFlows {

    public static void main(String[] args) throws Exception {
        union();
    }

    /**
     * 多条流合并
     * @throws Exception
     */
    public static void union() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<SensorReading> operatorA = environment.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner((event, timestamp) -> event.timestamp()));
        SingleOutputStreamOperator<SensorReading> operatorB = environment.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner((event, timestamp) -> event.timestamp()));
        SingleOutputStreamOperator<SensorReading> operatorC = environment.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner((event, timestamp) -> event.timestamp()));

        DataStream<SensorReading> operatorAll = operatorA.union(operatorB, operatorC);

        operatorAll.map(SensorReading::id).print();
        environment.execute();

    }


}
