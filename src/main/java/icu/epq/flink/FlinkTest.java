package icu.epq.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Flink 练习
 *
 * @author epqsky
 */
public class FlinkTest {

    static List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    public static void main(String[] args) throws Exception {
        processingTimeWindow();
    }

    /**
     * data stream api
     *
     * @throws Exception
     */
    private static void dataStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> source = env.addSource(new FromElementsFunction<>(Types.INT.createSerializer(env.getConfig()), data), Types.INT);
        DataStream<Integer> ds = source.map(value -> value * 2).keyBy(value -> 1).sum(0);
        ds.addSink(new PrintSinkFunction<>());
        System.out.println(env.getExecutionPlan());
        env.execute();
    }

    /**
     * 状态
     *
     * @throws Exception
     */
    private static void state() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromCollection(data)
                .keyBy(value -> value % 2)
                .process(new KeyedProcessFunction<Integer, Integer, Integer>() {

                    private ValueState<Integer> sumState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<Integer> sumDescriptor = new ValueStateDescriptor<>("Sum", Integer.class);
                        sumState = getRuntimeContext().getState(sumDescriptor);
                    }

                    @Override
                    public void processElement(Integer integer, Context context, Collector<Integer> collector) throws Exception {
                        Integer oldSum = sumState.value();
                        int sum = oldSum == null ? 0 : oldSum;
                        sum += integer;
                        sumState.update(sum);
                        collector.collect(sum);
                    }
                }).print().setParallelism(2);

        env.execute();
    }

    /**
     * 时间
     */
    private static void processingTimeWindow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env.addSource(new SourceFunction<Integer>() {

            private volatile boolean stop = false;

            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                int i = 0;
                while (!stop && i < data.size()) {
                    sourceContext.collect(data.get(i++));
                    Thread.sleep(200);
                }
            }

            @Override
            public void cancel() {
                stop = true;
            }
        }).setParallelism(1);
        source.keyBy(value -> value * 2).process(new KeyedProcessFunction<Integer, Integer, Integer>() {

            private static final int WINDOW_SIZE = 200;
            private TreeMap<Long, Integer> windows;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                windows = new TreeMap<>();
            }

            @Override
            public void processElement(Integer integer, Context context, Collector<Integer> collector) {
                long currentTime = context.timerService().currentProcessingTime();
                long windowStart = currentTime / WINDOW_SIZE;
                // 更新 window
                int sum = windows.getOrDefault(windowStart, 0);
                windows.put(windowStart, sum + integer);

                NavigableMap<Long, Integer> oldWindows = windows.headMap(windowStart, false);
                Iterator<Map.Entry<Long, Integer>> iterator = oldWindows.entrySet().iterator();
                while (iterator.hasNext()) {
                    collector.collect(iterator.next().getValue());
                    iterator.remove();
                }
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println(windows);
            }
        }).print().setParallelism(2);

        env.execute();
    }

}